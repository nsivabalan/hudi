#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Extract every Hudi configuration property from the source tree, with code context.

The published configuration reference tells a user a config's key, default and
description. It cannot tell them where the value is actually read, what else is
read beside it, or under what condition the read happens at all. That last part
is what makes a config silently do nothing. This script recovers it from source.

Three passes:

  1. Declarations. Parse ``ConfigProperty`` builder chains (and Flink's
     ``ConfigOptions`` chains whose key is a literal ``hoodie.*``) out of the
     main source trees. Chains span many lines and end at the statement
     semicolon, so the parser works on brace/paren-balanced statement text
     rather than on single lines.

  2. Accessors. Find config-class methods whose body is a single
     ``getInt(SOME_CONFIG)`` style read, so that call sites of
     ``getInlineCompactDeltaCommitMax()`` can be attributed back to
     ``hoodie.compact.inline.max.delta.commits``.

  3. Read sites. Walk every source file again, attributing each mention of a
     config constant or of a resolved accessor to the enclosing method, and
     recording the other configs mentioned in that same method (co-configs) and
     the enclosing ``if``/``switch``/``case`` conditions (gating). The gating
     pass is a textual heuristic; every condition it emits is marked as such.

Standard library only. Written to degrade: anything it cannot fully parse still
appears in the catalog, carrying a ``parseWarnings`` list.

Usage:
    python3 scripts/generate_config_catalog.py [--repo-root DIR] [--out-dir DIR]
"""

import argparse
import json
import os
import re
import subprocess
import sys
from collections import defaultdict, OrderedDict

# --------------------------------------------------------------------------
# Tree walking
# --------------------------------------------------------------------------

# Modules that declare configs. Anything under the repo root that is not one of
# these is still scanned for *read sites*; declarations are only looked for in
# main source trees, which SOURCE_DIR_MARKER enforces.
SOURCE_DIR_MARKER = os.path.join("src", "main")

EXCLUDED_PATH_PARTS = (
    os.sep + "target" + os.sep,
    os.sep + "test" + os.sep,
    os.sep + "src" + os.sep + "test" + os.sep,
    os.sep + "node_modules" + os.sep,
    os.sep + ".git" + os.sep,
)

# Directories we never descend into at all.
PRUNED_DIR_NAMES = {"target", "node_modules", ".git", ".idea", "docker", "rfc"}

READ_SITE_EXTENSIONS = (".java", ".scala")

MAX_READ_SITES = 20
MAX_CO_CONFIGS = 25
MAX_GATES_PER_SITE = 3


def is_excluded(path):
    normalized = os.sep + path.strip(os.sep) + os.sep
    return any(part in normalized for part in EXCLUDED_PATH_PARTS)


def walk_source_files(repo_root, extensions):
    """Yield absolute paths of non-test, non-generated source files."""
    for dirpath, dirnames, filenames in os.walk(repo_root):
        dirnames[:] = [d for d in dirnames if d not in PRUNED_DIR_NAMES]
        if is_excluded(dirpath):
            continue
        for filename in filenames:
            if filename.endswith(extensions):
                full = os.path.join(dirpath, filename)
                if not is_excluded(full):
                    yield full


# --------------------------------------------------------------------------
# Lightweight Java lexing helpers
# --------------------------------------------------------------------------

def strip_comments_and_strings(text, keep_string_bodies=False):
    """Blank out comments and (optionally) string bodies, preserving offsets.

    Offsets are preserved so that any index computed on the stripped text maps
    straight back onto the original. Newlines survive so line numbers hold.
    """
    out = list(text)
    i = 0
    n = len(text)
    while i < n:
        ch = text[i]
        if ch == "/" and i + 1 < n and text[i + 1] == "/":
            while i < n and text[i] != "\n":
                out[i] = " "
                i += 1
        elif ch == "/" and i + 1 < n and text[i + 1] == "*":
            out[i] = out[i + 1] = " "
            i += 2
            while i < n and not (text[i] == "*" and i + 1 < n and text[i + 1] == "/"):
                if text[i] != "\n":
                    out[i] = " "
                i += 1
            if i < n:
                out[i] = " "
                if i + 1 < n:
                    out[i + 1] = " "
                i += 2
        elif ch in ('"', "'"):
            quote = ch
            i += 1
            while i < n:
                if text[i] == "\\":
                    if not keep_string_bodies:
                        out[i] = " "
                        if i + 1 < n:
                            out[i + 1] = " "
                    i += 2
                    continue
                if text[i] == quote:
                    break
                if not keep_string_bodies and text[i] != "\n":
                    out[i] = " "
                i += 1
            i += 1
        else:
            i += 1
    return "".join(out)


def line_of(text, index):
    return text.count("\n", 0, index) + 1


def find_statement_end(text, start):
    """Index just past the ``;`` ending the statement that starts at ``start``.

    Respects nesting and ignores semicolons inside strings, chars and comments.
    """
    masked = strip_comments_and_strings(text[start:])
    depth_paren = depth_brace = depth_bracket = 0
    for offset, ch in enumerate(masked):
        if ch == "(":
            depth_paren += 1
        elif ch == ")":
            depth_paren -= 1
        elif ch == "{":
            depth_brace += 1
        elif ch == "}":
            depth_brace -= 1
        elif ch == "[":
            depth_bracket += 1
        elif ch == "]":
            depth_bracket -= 1
        elif ch == ";" and depth_paren <= 0 and depth_brace <= 0 and depth_bracket <= 0:
            return start + offset + 1
    return -1


def split_top_level_args(arg_text):
    """Split a call's argument text on top-level commas."""
    masked = strip_comments_and_strings(arg_text)
    args = []
    depth = 0
    current_start = 0
    for i, ch in enumerate(masked):
        if ch in "([{":
            depth += 1
        elif ch in ")]}":
            depth -= 1
        elif ch == "," and depth == 0:
            args.append(arg_text[current_start:i].strip())
            current_start = i + 1
    tail = arg_text[current_start:].strip()
    if tail:
        args.append(tail)
    return args


def extract_call_args(text, call_start):
    """Given an index at the ``(`` of a call, return (args_text, index_past_close)."""
    masked = strip_comments_and_strings(text)
    if call_start >= len(text) or text[call_start] != "(":
        return None, call_start
    depth = 0
    for i in range(call_start, len(masked)):
        if masked[i] == "(":
            depth += 1
        elif masked[i] == ")":
            depth -= 1
            if depth == 0:
                return text[call_start + 1:i], i + 1
    return None, call_start


# --------------------------------------------------------------------------
# Java string-literal evaluation
# --------------------------------------------------------------------------

_JAVA_ESCAPES = {
    "n": "\n", "t": "\t", "r": "\r", "b": "\b", "f": "\f",
    '"': '"', "'": "'", "\\": "\\", "0": "\0",
}

STRING_LITERAL_RE = re.compile(r'"((?:[^"\\]|\\.)*)"', re.DOTALL)


def unescape_java(raw):
    out = []
    i = 0
    while i < len(raw):
        if raw[i] == "\\" and i + 1 < len(raw):
            nxt = raw[i + 1]
            if nxt == "u":
                try:
                    out.append(chr(int(raw[i + 2:i + 6], 16)))
                    i += 6
                    continue
                except ValueError:
                    pass
            out.append(_JAVA_ESCAPES.get(nxt, nxt))
            i += 2
        else:
            out.append(raw[i])
            i += 1
    return "".join(out)


IDENT_PATH_RE = re.compile(r"[A-Za-z_$][A-Za-z0-9_$]*(?:\s*\.\s*[A-Za-z_$][A-Za-z0-9_$]*)*")


def concatenated_string_literal(expr, string_constants=None, owner_class=None, _depth=0):
    """Evaluate a Java expression that is a concatenation of string literals.

    With ``string_constants`` supplied, identifiers that name a ``static final
    String`` constant are substituted too, which is how the many
    ``SOME_PREFIX + "suffix"`` config keys get resolved. Returns None when the
    expression contains anything else, so that callers can distinguish "a
    literal we read exactly" from "something computed".
    """
    expr = expr.strip()
    if not expr or _depth > 8:
        return None
    pieces = []
    pos = 0
    saw_literal = False
    while pos < len(expr):
        match = STRING_LITERAL_RE.match(expr, pos)
        if match:
            pieces.append(unescape_java(match.group(1)))
            saw_literal = True
            pos = match.end()
            continue
        ch = expr[pos]
        if ch.isspace() or ch == "+":
            pos += 1
            continue
        if string_constants is not None:
            ident = IDENT_PATH_RE.match(expr, pos)
            if ident:
                resolved = lookup_string_constant(
                    ident.group(0), string_constants, owner_class, _depth)
                if resolved is not None:
                    pieces.append(resolved)
                    saw_literal = True
                    pos = ident.end()
                    continue
        return None
    return "".join(pieces) if saw_literal else None


def lookup_string_constant(path, string_constants, owner_class, depth):
    """Resolve ``PREFIX`` / ``Holder.PREFIX`` to its literal value, recursively."""
    parts = [p.strip() for p in path.split(".")]
    const = parts[-1]
    qualifier = parts[-2] if len(parts) >= 2 else None
    for candidate in ((qualifier, const), (owner_class, const), (None, const)):
        expression = string_constants.get(candidate)
        if expression is None:
            continue
        resolved = concatenated_string_literal(
            expression, string_constants, candidate[0] or owner_class, depth + 1)
        if resolved is not None:
            return resolved
    return None


ENUM_DESCRIPTION_RE = re.compile(r"@EnumDescription\s*\(")
ENUM_FIELD_DESCRIPTION_RE = re.compile(r"@EnumFieldDescription\s*\(")
ENUM_DECL_RE = re.compile(r"\benum\s+([A-Za-z_$][A-Za-z0-9_$]*)")
# The last constant in an enum body is followed by `}` rather than `,` or `;`.
ENUM_CONSTANT_RE = re.compile(r"\A\s*([A-Z][A-Z0-9_]*)\s*[,;(){}]")


def collect_enum_documentation(repo_root):
    """Pass 0b. Enum name -> its ``@EnumDescription`` and per-constant descriptions.

    ``withDocumentation(SomeEnum.class)`` means the user-facing text for that
    config lives on the enum, not at the declaration. Without this the catalog
    would report "no documentation" for exactly the configs whose valid values
    matter most.
    """
    documented = {}
    for file_path in walk_source_files(repo_root, (".java",)):
        if SOURCE_DIR_MARKER not in file_path:
            continue
        try:
            with open(file_path, "r", encoding="utf-8", errors="replace") as handle:
                original = handle.read()
        except OSError:
            continue
        if "@EnumDescription" not in original:
            continue
        masked = strip_comments_and_strings(original, keep_string_bodies=True)
        for match in ENUM_DESCRIPTION_RE.finditer(masked):
            args, after = extract_call_args(masked, match.end() - 1)
            if args is None:
                continue
            enum_match = ENUM_DECL_RE.search(masked, after)
            if not enum_match:
                continue
            name = enum_match.group(1)
            body_start = masked.find("{", enum_match.end())
            if body_start < 0:
                continue
            depth = 0
            body_end = len(masked)
            for i in range(body_start, len(masked)):
                if masked[i] == "{":
                    depth += 1
                elif masked[i] == "}":
                    depth -= 1
                    if depth == 0:
                        body_end = i
                        break
            values = []
            for field in ENUM_FIELD_DESCRIPTION_RE.finditer(masked, body_start, body_end):
                field_args, field_after = extract_call_args(masked, field.end() - 1)
                if field_args is None:
                    continue
                tail = masked[field_after:field_after + 160]
                constant = ENUM_CONSTANT_RE.match(tail)
                if not constant:
                    continue
                values.append(OrderedDict([
                    ("value", constant.group(1)),
                    ("description", concatenated_string_literal(field_args) or ""),
                ]))
            documented[name] = OrderedDict([
                ("enum", name),
                ("file", os.path.relpath(file_path, repo_root)),
                ("description", concatenated_string_literal(args) or ""),
                ("values", values),
            ])
    return documented


STRING_CONSTANT_RE = re.compile(
    r"\bstatic\s+final\s+String\s+(?P<name>[A-Za-z_$][A-Za-z0-9_$]*)\s*=\s*(?P<value>[^;]*);")


def collect_string_constants(repo_root):
    """Pass 0. (class, constant) -> its initializer expression, for key prefixes.

    Keyed both by owning class and by bare name. A bare name claimed by two
    different expressions is dropped, so an ambiguous prefix never silently
    resolves to the wrong value.
    """
    by_qualified = {}
    bare = {}
    bare_conflicts = set()
    for file_path in walk_source_files(repo_root, (".java",)):
        if SOURCE_DIR_MARKER not in file_path:
            continue
        try:
            with open(file_path, "r", encoding="utf-8", errors="replace") as handle:
                original = handle.read()
        except OSError:
            continue
        if "static final String" not in original:
            continue
        masked = strip_comments_and_strings(original, keep_string_bodies=True)
        for match in STRING_CONSTANT_RE.finditer(masked):
            name = match.group("name")
            value = match.group("value").strip()
            if not value or len(value) > 400:
                continue
            owner = find_enclosing_class(masked, match.start())
            by_qualified[(owner, name)] = value
            if name in bare and bare[name] != value:
                bare_conflicts.add(name)
            bare[name] = value
    for name in bare_conflicts:
        bare.pop(name, None)
    constants = {(None, name): value for name, value in bare.items()}
    constants.update(by_qualified)
    return constants


def literal_or_expression(expr):
    """Return (value, is_literal) for a default-value expression."""
    literal = concatenated_string_literal(expr)
    if literal is not None:
        return literal, True
    collapsed = " ".join(expr.split())
    if re.fullmatch(r"(true|false)", collapsed):
        return collapsed, True
    if re.fullmatch(r"-?\d+[LlFfDd]?", collapsed):
        return collapsed.rstrip("LlFfDd"), True
    if re.fullmatch(r"-?\d*\.\d+[FfDd]?", collapsed):
        return collapsed.rstrip("FfDd"), True
    return collapsed, False


# --------------------------------------------------------------------------
# Pass 1 -- config declarations
# --------------------------------------------------------------------------

# Matches both `ConfigProperty<String> FOO = ConfigProperty` and the Flink
# `ConfigOption<String> FOO = ConfigOptions` forms, plus the rarer single-line
# `= ConfigProperty.key(...)`.
DECLARATION_RE = re.compile(
    r"(?P<decl>(?:public|protected|private)?\s*(?:static\s+)?(?:final\s+)?"
    r"(?P<holder>ConfigProperty|ConfigOption)\s*<\s*(?P<type>[^>]*(?:<[^>]*>)?[^>]*)\s*>\s+"
    r"(?P<name>[A-Za-z_$][A-Za-z0-9_$]*)\s*=\s*)"
    r"(?P<builder>ConfigProperty|ConfigOptions)\b"
)

CLASS_DECL_RE = re.compile(
    r"\b(?:public|protected|private)?\s*(?:static\s+)?(?:final\s+)?(?:abstract\s+)?"
    r"(?:class|interface|enum)\s+([A-Za-z_$][A-Za-z0-9_$]*)"
)

# Builder steps we care about, applied to the chain text after the key.
CHAIN_STEP_RE = re.compile(r"\.\s*([A-Za-z_$][A-Za-z0-9_$]*)\s*\(")


def find_enclosing_class(text, index):
    """Innermost-looking class name declared before ``index``."""
    best = None
    for match in CLASS_DECL_RE.finditer(text, 0, index):
        best = match.group(1)
    return best


def parse_chain_steps(chain_text):
    """Return an ordered list of (method_name, args_text) from a builder chain.

    Comments are blanked first -- authors do put explanatory comments between a
    builder call's arguments, and those must not end up inside an extracted
    value. String bodies are kept so literals survive.
    """
    chain_text = strip_comments_and_strings(chain_text, keep_string_bodies=True)
    steps = []
    pos = 0
    while True:
        match = CHAIN_STEP_RE.search(chain_text, pos)
        if not match:
            break
        args, after = extract_call_args(chain_text, match.end() - 1)
        if args is None:
            pos = match.end()
            continue
        steps.append((match.group(1), args))
        pos = after
    return steps


DERIVED_KEY_RE = re.compile(
    r"([A-Za-z_$][A-Za-z0-9_$.]*)\s*\.\s*key\(\s*\)\s*(?:\+\s*(.+))?")


def derived_from_config_key(expr, string_constants, owner_class, base_key_resolver):
    """Resolve `OTHER_CONFIG.key()` and `OTHER_CONFIG.key() + ".suffix"`.

    Several configs name their own key, and several name an alternative key,
    relative to another config's. Returns None when the base config is unknown.
    """
    match = DERIVED_KEY_RE.fullmatch(" ".join(expr.split()))
    if not match:
        return None
    base = base_key_resolver(match.group(1))
    if base is None:
        return None
    if match.group(2) is None:
        return base
    suffix = concatenated_string_literal(match.group(2), string_constants, owner_class)
    return None if suffix is None else base + suffix


def parse_declaration(masked_for_scan, original, match, file_path, repo_root,
                      string_constants, base_key_resolver=lambda path: None,
                      enum_docs=None):
    """Build one catalog entry from a declaration match. Never raises."""
    warnings = []
    start = match.start()
    end = find_statement_end(original, start)
    if end < 0:
        end = min(len(original), start + 4000)
        warnings.append("statement terminator not found; chain truncated at 4000 chars")
    chain_text = original[start:end]

    steps = parse_chain_steps(chain_text)
    step_names = [name for name, _ in steps]

    enclosing_class = find_enclosing_class(masked_for_scan, start)

    key = None
    key_expr = None
    key_resolution = None
    for name, args in steps:
        if name == "key":
            key_expr = args.strip()
            key = concatenated_string_literal(args)
            if key is not None:
                key_resolution = "literal"
            else:
                key = concatenated_string_literal(
                    args, string_constants, enclosing_class)
                if key is not None:
                    key_resolution = "resolvedFromConstants"
            break

    if key is None and key_expr:
        # Flink re-exports read the key off another ConfigProperty, and some
        # Java configs build the key from a shared prefix constant.
        reference = re.fullmatch(
            r"([A-Za-z_$][A-Za-z0-9_$.]*)\s*\.\s*key\(\s*\)", key_expr.strip())
        if reference:
            return None, "alias:" + reference.group(1)
        # `OTHER_CONFIG.key() + ".suffix"` -- a key derived from another config's.
        key = derived_from_config_key(
            key_expr, string_constants, enclosing_class, base_key_resolver)
        if key is not None:
            key_resolution = "derivedFromConfigKey"
        else:
            warnings.append(
                "key is a computed expression: " + " ".join(key_expr.split())[:160])

    entry = OrderedDict()
    entry["key"] = key
    entry["keyResolution"] = key_resolution
    entry["keyExpression"] = (" ".join(key_expr.split())
                              if key_expr and key_resolution != "literal" else None)
    entry["type"] = " ".join(match.group("type").split())
    entry["declaredIn"] = OrderedDict([
        ("class", enclosing_class),
        ("constant", match.group("name")),
        ("file", os.path.relpath(file_path, repo_root)),
        ("line", line_of(original, start)),
    ])
    entry["configGroup"] = entry["declaredIn"]["class"]
    entry["builderStyle"] = "ConfigProperty" if match.group("builder") == "ConfigProperty" else "FlinkConfigOptions"

    # Default value.
    default_value = None
    default_is_literal = False
    has_default = None
    for name, args in steps:
        if name == "defaultValue":
            parts = split_top_level_args(args)
            if parts:
                default_value, default_is_literal = literal_or_expression(parts[0])
                has_default = True
                if len(parts) > 1:
                    doc_on_default = concatenated_string_literal(parts[1])
                    if doc_on_default:
                        entry["docOnDefaultValue"] = doc_on_default
            break
        if name == "noDefaultValue":
            has_default = False
            break
    if has_default is None:
        has_default = False
        if entry["builderStyle"] == "ConfigProperty":
            warnings.append("neither defaultValue() nor noDefaultValue() found in chain")
    entry["hasDefaultValue"] = has_default
    entry["defaultValue"] = default_value
    entry["defaultValueIsLiteral"] = default_is_literal
    if has_default and not default_is_literal:
        entry["defaultValueNote"] = "computed at class-init time; shown as the source expression"

    # Documentation: a string, or a class reference whose text lives elsewhere.
    documentation = None
    documentation_source = None
    enum_documentation = None
    for name, args in steps:
        if name not in ("withDocumentation", "withDescription"):
            continue
        parts = split_top_level_args(args)
        if not parts:
            continue
        class_ref = re.fullmatch(r"([A-Za-z_$][A-Za-z0-9_$.]*)\s*\.\s*class", parts[0].strip())
        if class_ref:
            enum_name = class_ref.group(1).split(".")[-1]
            documentation_source = "enumClass:" + enum_name
            extra = concatenated_string_literal(parts[1]) if len(parts) > 1 else None
            described = (enum_docs or {}).get(enum_name)
            if described:
                entry_enum = described
                pieces = [p for p in (extra, described["description"]) if p]
                documentation = "\n".join(pieces)
                enum_documentation = entry_enum
            else:
                documentation = extra
                warnings.append(
                    "documentation points at enum %s, whose @EnumDescription was not found"
                    % enum_name)
        else:
            documentation = concatenated_string_literal(parts[0])
            documentation_source = "literal"
            if documentation is None:
                documentation = concatenated_string_literal(
                    parts[0], string_constants, enclosing_class)
                documentation_source = "resolvedFromConstants"
            if documentation is None:
                documentation_source = None
                warnings.append("documentation is a computed expression")
        break
    entry["documentation"] = documentation
    entry["documentationSource"] = documentation_source
    entry["enumDocumentation"] = enum_documentation

    def collect_string_args(step_name):
        for name, args in steps:
            if name == step_name:
                values = []
                for part in split_top_level_args(args):
                    literal = concatenated_string_literal(part)
                    if literal is None:
                        literal = concatenated_string_literal(
                            part, string_constants, enclosing_class)
                    if literal is None:
                        literal = derived_from_config_key(
                            part, string_constants, enclosing_class, base_key_resolver)
                    values.append(literal if literal is not None
                                  else " ".join(part.split()))
                return values
        return []

    entry["validValues"] = collect_string_args("withValidValues")
    # A config whose documentation is an enum class declares its permitted values on the enum
    # rather than via withValidValues(...). Fall back to the enum's constants so that
    # "what can I set this to?" is answerable for strategy/policy configs, which are exactly
    # the ones where it matters most.
    if not entry["validValues"] and enum_documentation:
        entry["validValues"] = [v["value"] for v in enum_documentation.get("values", []) if v.get("value")]
    entry["alternatives"] = collect_string_args("withAlternatives")
    since = collect_string_args("sinceVersion")
    entry["sinceVersion"] = since[0] if since else None
    deprecated = collect_string_args("deprecatedAfter")
    entry["deprecatedAfter"] = deprecated[0] if deprecated else None
    entry["supportedVersions"] = collect_string_args("supportedVersions")
    entry["advanced"] = "markAdvanced" in step_names
    entry["hasInferFunction"] = "withInferFunction" in step_names
    entry["builderSteps"] = step_names

    if key is None:
        warnings.append("key could not be resolved to a string literal")
    if documentation is None and documentation_source != "enumClass":
        warnings.append("no documentation text resolved")

    entry["parseWarnings"] = warnings
    return entry, None


def collect_declarations(repo_root, verbose=False):
    """Pass 1. Returns (entries, constant_index, stats)."""
    entries = []
    # (file-local class, constant name) -> config key, so later passes can map a
    # constant reference back to the config it names.
    constant_index = {}
    alias_pending = []
    stats = defaultdict(int)

    string_constants = collect_string_constants(repo_root)
    stats["stringConstants"] = len(string_constants)
    enum_docs = collect_enum_documentation(repo_root)
    stats["documentedEnums"] = len(enum_docs)
    if verbose:
        print("  string constants indexed: %d" % len(string_constants), file=sys.stderr)
        print("  documented enums indexed: %d" % len(enum_docs), file=sys.stderr)

    for file_path in walk_source_files(repo_root, (".java",)):
        if SOURCE_DIR_MARKER not in file_path:
            continue
        try:
            with open(file_path, "r", encoding="utf-8", errors="replace") as handle:
                original = handle.read()
        except OSError:
            stats["unreadableFiles"] += 1
            continue
        if "ConfigProperty" not in original and "ConfigOptions" not in original:
            continue

        masked = strip_comments_and_strings(original)

        def resolve_base_key(path, _index=constant_index):
            parts = path.split(".")
            const = parts[-1]
            qualifier = parts[-2] if len(parts) >= 2 else None
            return _index.get((qualifier, const)) or _index.get((None, const))

        for match in DECLARATION_RE.finditer(masked):
            stats["declarationsSeen"] += 1
            entry, alias_target = parse_declaration(
                masked, original, match, file_path, repo_root, string_constants,
                resolve_base_key, enum_docs)
            if alias_target is not None:
                alias_pending.append((
                    find_enclosing_class(masked, match.start()),
                    match.group("name"),
                    alias_target.split(":", 1)[1],
                    os.path.relpath(file_path, repo_root),
                    line_of(original, match.start()),
                ))
                continue
            if entry is None:
                stats["unparseable"] += 1
                continue
            # Flink ConfigOptions that do not name a hoodie.* key belong to the
            # Flink option namespace, not the Hudi config namespace.
            if entry["builderStyle"] == "FlinkConfigOptions" and not (
                    entry["key"] or "").startswith("hoodie."):
                stats["flinkOptionsSkipped"] += 1
                continue
            entries.append(entry)
            owner = entry["declaredIn"]["class"]
            if entry["key"]:
                constant_index[(owner, entry["declaredIn"]["constant"])] = entry["key"]
                constant_index[(None, entry["declaredIn"]["constant"])] = entry["key"]

    # Resolve re-export aliases (`FOO = OtherHolder.FOO`) to the real key so a
    # read site that uses the alias still lands on the right config.
    aliases = []
    for owner, name, target, rel_file, line in alias_pending:
        target_parts = target.split(".")
        target_const = target_parts[-1]
        target_class = target_parts[-2] if len(target_parts) >= 2 else None
        key = constant_index.get((target_class, target_const)) or \
            constant_index.get((None, target_const))
        if key:
            constant_index[(owner, name)] = key
            aliases.append(OrderedDict([
                ("key", key), ("aliasClass", owner), ("aliasConstant", name),
                ("file", rel_file), ("line", line)]))
            stats["aliasesResolved"] += 1
        else:
            stats["aliasesUnresolved"] += 1

    return entries, constant_index, aliases, stats


# --------------------------------------------------------------------------
# Pass 2 -- accessor methods
# --------------------------------------------------------------------------

# Methods and constructors, including package-private ones. The modifier is
# optional, so NON_METHOD_KEYWORDS below keeps `if (...) {` and friends out.
METHOD_SIGNATURE_RE = re.compile(
    r"(?:(?:public|protected|private)\s+)?(?:static\s+|final\s+|synchronized\s+|abstract\s+)*"
    r"(?:(?P<ret>[A-Za-z_$][A-Za-z0-9_$<>,.\[\]\s?]*?)\s+)?"
    r"(?P<name>[A-Za-z_$][A-Za-z0-9_$]*)\s*\((?P<params>[^;{()]*(?:\([^()]*\)[^;{()]*)*)\)\s*"
    r"(?:throws\s+[A-Za-z0-9_$.,\s]+)?\{"
)

NON_METHOD_KEYWORDS = frozenset((
    "if", "else", "for", "while", "switch", "catch", "try", "do", "synchronized",
    "return", "new", "case", "assert", "super", "this",
))

GETTER_CALL_RE = re.compile(
    r"\bget(?:String|Int|Integer|Long|Boolean|Double|Float|StringOrDefault|"
    r"IntOrDefault|BooleanOrDefault|LongOrDefault|Enum|Class)?\s*\(\s*"
    r"(?P<arg>[A-Za-z_$][A-Za-z0-9_$.]*)\s*[,)]"
)

ALT_KEYS_CALL_RE = re.compile(
    r"\b(?:get|contains)(?:String|Int|Integer|Long|Boolean|Double|Raw)?WithAltKeys\s*\([^)]*?"
    r"\b(?P<arg>[A-Za-z_$][A-Za-z0-9_$.]*)\s*[,)]"
)


def find_method_bodies(masked, original):
    """Yield (name, body_start, body_end, signature_line) for each method.

    ``body_start`` indexes the ``{``; ``body_end`` the matching ``}``.
    """
    for match in METHOD_SIGNATURE_RE.finditer(masked):
        if match.group("name") in NON_METHOD_KEYWORDS:
            continue
        if (match.group("ret") or "").split()[-1:] and \
                (match.group("ret") or "").split()[-1] in NON_METHOD_KEYWORDS:
            continue
        brace_index = masked.find("{", match.end() - 1)
        if brace_index < 0:
            continue
        depth = 0
        end = -1
        for i in range(brace_index, len(masked)):
            if masked[i] == "{":
                depth += 1
            elif masked[i] == "}":
                depth -= 1
                if depth == 0:
                    end = i
                    break
        if end < 0:
            continue
        yield match.group("name"), brace_index, end, line_of(original, match.start())


def resolve_constant(expr, owner_class, constant_index):
    """Map a (possibly qualified) constant reference to a config key."""
    parts = expr.split(".")
    const = parts[-1]
    qualifier = parts[-2] if len(parts) >= 2 else None
    return (constant_index.get((qualifier, const))
            or (constant_index.get((owner_class, const)) if qualifier is None else None)
            or constant_index.get((None, const)))


def collect_accessors(repo_root, constant_index):
    """Pass 2. accessor method name -> set of config keys it reads.

    Deliberately narrow: only methods whose body is short and mentions exactly
    one config constant, so that a call site of the method is unambiguous.
    """
    accessors = defaultdict(set)
    accessor_sites = {}
    for file_path in walk_source_files(repo_root, (".java",)):
        if SOURCE_DIR_MARKER not in file_path:
            continue
        try:
            with open(file_path, "r", encoding="utf-8", errors="replace") as handle:
                original = handle.read()
        except OSError:
            continue
        if "get" not in original or "Config" not in original:
            continue
        masked = strip_comments_and_strings(original)
        owner_class = None
        class_match = CLASS_DECL_RE.search(masked)
        if class_match:
            owner_class = class_match.group(1)

        for name, body_start, body_end, sig_line in find_method_bodies(masked, original):
            body = masked[body_start:body_end]
            if len(body) > 600:
                continue
            keys = set()
            for pattern in (GETTER_CALL_RE, ALT_KEYS_CALL_RE):
                for call in pattern.finditer(body):
                    key = resolve_constant(call.group("arg"), owner_class, constant_index)
                    if key:
                        keys.add(key)
            if len(keys) == 1:
                only = next(iter(keys))
                accessors[name].add(only)
                accessor_sites.setdefault(name, (owner_class, os.path.relpath(file_path, repo_root), sig_line))
    # An accessor name that maps to more than one key across classes is
    # ambiguous at a call site; drop it rather than mis-attribute.
    return ({name: next(iter(keys)) for name, keys in accessors.items() if len(keys) == 1},
            accessor_sites)


# --------------------------------------------------------------------------
# Pass 3 -- read sites, co-configs, gating
# --------------------------------------------------------------------------

IDENTIFIER_RE = re.compile(r"\b([A-Za-z_$][A-Za-z0-9_$]*)\s*(?:\.\s*([A-Za-z_$][A-Za-z0-9_$]*))?")

IF_RE = re.compile(r"\bif\s*\(")
SWITCH_RE = re.compile(r"\bswitch\s*\(")
CASE_RE = re.compile(r"\bcase\s+([A-Za-z_$][A-Za-z0-9_$.]*)\s*:")

# Strings that mean "this read only happens under a condition", used to decide
# whether a gate is worth recording at all.
CONDITION_NOISE_RE = re.compile(r"^\s*(true|false|1|0)\s*$")


def summarize_condition(condition):
    collapsed = " ".join(condition.split())
    if len(collapsed) > 160:
        collapsed = collapsed[:157] + "..."
    return collapsed


def enclosing_gates(masked, body_start, site_index):
    """Best-effort: conditions of the if/switch/case blocks containing ``site_index``.

    Textual, not an AST walk. It finds each ``if (...)`` / ``switch (...)``
    before the site, locates the block it opens, and keeps the condition when
    the site falls inside that block. ``case`` labels are matched by scanning
    backwards to the nearest label within the enclosing switch.
    """
    gates = []

    for pattern, kind in ((IF_RE, "if"), (SWITCH_RE, "switch")):
        for match in pattern.finditer(masked, body_start, site_index):
            condition, after = extract_call_args(masked, match.end() - 1)
            if condition is None:
                continue
            brace = masked.find("{", after)
            if brace < 0 or brace > site_index:
                continue
            # Nothing but whitespace may sit between ")" and "{" for the brace
            # to be this construct's own block.
            if masked[after:brace].strip():
                continue
            depth = 0
            block_end = -1
            for i in range(brace, len(masked)):
                if masked[i] == "{":
                    depth += 1
                elif masked[i] == "}":
                    depth -= 1
                    if depth == 0:
                        block_end = i
                        break
            if block_end < 0 or not (brace < site_index < block_end):
                continue
            text = summarize_condition(condition)
            if CONDITION_NOISE_RE.match(text):
                continue
            if kind == "switch":
                label = None
                for case_match in CASE_RE.finditer(masked, brace, site_index):
                    label = case_match.group(1)
                if label:
                    gates.append("switch (%s) case %s" % (text, label))
                else:
                    gates.append("switch (%s)" % text)
            else:
                gates.append("if (%s)" % text)

    # Innermost conditions are the informative ones.
    return gates[-MAX_GATES_PER_SITE:]


def collect_read_sites(repo_root, constant_index, accessors, config_keys, verbose=False):
    """Pass 3. Returns key -> {"readSites": [...], "coConfigs": {key: count}}."""
    result = defaultdict(lambda: {"readSites": [], "coConfigs": defaultdict(int)})
    # Constant names that are unambiguous enough to attribute without a
    # qualifier: a bare name used by exactly one config.
    bare_constants = defaultdict(set)
    for (qualifier, const), key in constant_index.items():
        bare_constants[const].add(key)
    unambiguous_bare = {const: next(iter(keys))
                        for const, keys in bare_constants.items() if len(keys) == 1}

    stats = defaultdict(int)

    for file_path in walk_source_files(repo_root, READ_SITE_EXTENSIONS):
        try:
            with open(file_path, "r", encoding="utf-8", errors="replace") as handle:
                original = handle.read()
        except OSError:
            continue
        masked = strip_comments_and_strings(original)
        relative = os.path.relpath(file_path, repo_root)
        owner_class = None
        class_match = CLASS_DECL_RE.search(masked)
        if class_match:
            owner_class = class_match.group(1)

        if file_path.endswith(".scala"):
            # Scala has no method-body parser here; attribute to the file and
            # skip gating rather than guess.
            for match in IDENTIFIER_RE.finditer(masked):
                key, via = _identifier_to_key(match, owner_class, constant_index,
                                              unambiguous_bare, accessors)
                if not key or key not in config_keys:
                    continue
                bucket = result[key]
                if len(bucket["readSites"]) < MAX_READ_SITES:
                    bucket["readSites"].append(OrderedDict([
                        ("class", owner_class),
                        ("method", None),
                        ("file", relative),
                        ("line", line_of(original, match.start())),
                        ("via", "scalaReference"),
                        ("gates", []),
                    ]))
                    stats["readSites"] += 1
            continue

        declaring_file = relative

        for method_name, body_start, body_end, sig_line in find_method_bodies(masked, original):
            body = masked[body_start:body_end]
            # Which configs does this method touch at all? Needed for co-configs.
            hits = []
            for match in IDENTIFIER_RE.finditer(body):
                key, via = _identifier_to_key(match, owner_class, constant_index,
                                              unambiguous_bare, accessors)
                if key and key in config_keys:
                    hits.append((key, body_start + match.start(), via))
            if not hits:
                continue

            touched = OrderedDict()
            for key, _, _ in hits:
                touched[key] = True

            # A value read into a local is almost always *used* further down,
            # often inside the branch that decides whether it matters at all.
            # Following the local is what surfaces the real gate.
            local_uses = local_alias_uses(body, body_start, hits)

            seen_in_method = set()
            for key, absolute_index, via in hits:
                bucket = result[key]
                # One read site per (method, key); the first mention wins.
                if key in seen_in_method:
                    continue
                seen_in_method.add(key)

                for other in touched:
                    if other != key:
                        bucket["coConfigs"][other] += 1

                if len(bucket["readSites"]) >= MAX_READ_SITES:
                    stats["readSitesCapped"] += 1
                    continue
                # Reading the declaration itself is not a read site.
                if declaring_file.endswith(".java") and via == "constant" \
                        and _is_declaration_site(masked, absolute_index):
                    continue

                gates = list(enclosing_gates(masked, body_start, absolute_index))
                for use_index in local_uses.get(absolute_index, ()):
                    for gate in enclosing_gates(masked, body_start, use_index):
                        if gate not in gates:
                            gates.append(gate)

                bucket["readSites"].append(OrderedDict([
                    ("class", owner_class),
                    ("method", method_name),
                    ("file", declaring_file),
                    ("line", line_of(original, absolute_index)),
                    ("via", via),
                    ("gates", gates[:MAX_GATES_PER_SITE * 2]),
                ]))
                stats["readSites"] += 1

    return result, stats


LOCAL_ASSIGNMENT_RE = re.compile(
    r"(?:\b(?:final\s+)?[A-Za-z_$][A-Za-z0-9_$<>,.\[\]\s?]*?\s+)?"
    r"(?P<name>[A-Za-z_$][A-Za-z0-9_$]*)\s*=\s*$")


def local_alias_uses(body, body_start, hits):
    """Map a read site to later uses of the local variable it is assigned to.

    ``int inlineCompactDeltaCommitMax = config.getInlineCompactDeltaCommitMax();``
    puts the value in a local, and the branch that decides whether the config
    matters tests that local, not the accessor. Without this the gate for such a
    config would always come back empty.
    """
    uses = defaultdict(list)
    for _, absolute_index, _ in hits:
        relative = absolute_index - body_start
        line_start = body.rfind("\n", 0, relative) + 1
        prefix = body[line_start:relative]
        match = LOCAL_ASSIGNMENT_RE.search(prefix)
        if not match:
            continue
        name = match.group("name")
        if len(name) < 3:
            continue
        for occurrence in re.finditer(r"\b%s\b" % re.escape(name), body):
            if occurrence.start() <= relative:
                continue
            uses[absolute_index].append(body_start + occurrence.start())
            if len(uses[absolute_index]) >= 12:
                break
    return uses


def _is_declaration_site(masked, index):
    line_start = masked.rfind("\n", 0, index) + 1
    line_end = masked.find("\n", index)
    line = masked[line_start:line_end if line_end > 0 else len(masked)]
    return "ConfigProperty" in line and "=" in line


def _identifier_to_key(match, owner_class, constant_index, unambiguous_bare, accessors):
    """Map one identifier occurrence to a config key, or None.

    Returns (key, via). ``via`` records how the attribution was made so a reader
    can tell a direct constant reference from a call through an accessor.
    """
    first, second = match.group(1), match.group(2)
    if second:
        # `HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS`
        key = constant_index.get((first, second))
        if key:
            return key, "constant"
        # `config.getInlineCompactDeltaCommitMax()` -- the receiver is a value,
        # so only the method name carries the attribution.
        if second in accessors:
            return accessors[second], "accessor:" + second
        if _looks_like_constant(second):
            key = unambiguous_bare.get(second)
            if key:
                return key, "constant"
        return None, None
    if first in accessors:
        return accessors[first], "accessor:" + first
    if _looks_like_constant(first):
        key = unambiguous_bare.get(first)
        if key:
            return key, "constant"
    return None, None


def _looks_like_constant(name):
    return name.upper() == name and any(ch.isalpha() for ch in name)


# --------------------------------------------------------------------------
# Assembly and output
# --------------------------------------------------------------------------

def read_project_version(repo_root):
    pom = os.path.join(repo_root, "pom.xml")
    try:
        with open(pom, "r", encoding="utf-8") as handle:
            text = handle.read()
    except OSError:
        return None
    # The project's own version is the first <version> after the hudi artifactId.
    anchor = text.find("<artifactId>hudi</artifactId>")
    if anchor < 0:
        anchor = 0
    match = re.search(r"<version>([^<]+)</version>", text[anchor:])
    return match.group(1).strip() if match else None


def read_git_sha(repo_root):
    try:
        output = subprocess.run(
            ["git", "-C", repo_root, "rev-parse", "HEAD"],
            capture_output=True, text=True, timeout=30, check=False)
        return output.stdout.strip() or None
    except (OSError, subprocess.SubprocessError):
        return None


def module_of(relative_path):
    return relative_path.split(os.sep, 1)[0] if os.sep in relative_path else relative_path


def build_catalog(repo_root, verbose=False):
    entries, constant_index, aliases, decl_stats = collect_declarations(repo_root, verbose)
    if verbose:
        print("  declarations parsed: %d" % len(entries), file=sys.stderr)

    accessors, accessor_sites = collect_accessors(repo_root, constant_index)
    if verbose:
        print("  accessor methods resolved: %d" % len(accessors), file=sys.stderr)

    # Merge duplicate keys (same key declared in several classes -- re-exports
    # that the alias pass could not fold, or genuinely duplicated declarations).
    by_key = OrderedDict()
    keyless = []
    for entry in entries:
        key = entry["key"]
        if not key:
            keyless.append(entry)
            continue
        if key in by_key:
            existing = by_key[key]
            existing.setdefault("alsoDeclaredIn", []).append(entry["declaredIn"])
            # Prefer whichever declaration carries documentation.
            if not existing.get("documentation") and entry.get("documentation"):
                existing["documentation"] = entry["documentation"]
                existing["documentationSource"] = entry["documentationSource"]
            continue
        by_key[key] = entry

    config_keys = set(by_key)
    read_data, read_stats = collect_read_sites(
        repo_root, constant_index, accessors, config_keys, verbose)

    alias_by_key = defaultdict(list)
    for alias in aliases:
        alias_by_key[alias["key"]].append(alias)

    accessor_by_key = defaultdict(list)
    for name, key in accessors.items():
        owner, rel_file, line = accessor_sites.get(name, (None, None, None))
        accessor_by_key[key].append(OrderedDict([
            ("method", name), ("class", owner), ("file", rel_file), ("line", line)]))

    configs = []
    for key in sorted(by_key):
        entry = by_key[key]
        entry["module"] = module_of(entry["declaredIn"]["file"])
        entry["accessors"] = sorted(accessor_by_key.get(key, []),
                                    key=lambda a: (a["class"] or "", a["method"]))
        entry["aliasConstants"] = alias_by_key.get(key, [])
        data = read_data.get(key)
        if data:
            entry["readSites"] = data["readSites"]
            co_configs = sorted(data["coConfigs"].items(), key=lambda kv: (-kv[1], kv[0]))
            entry["coConfigs"] = [OrderedDict([("key", k), ("sharedMethods", c)])
                                  for k, c in co_configs[:MAX_CO_CONFIGS]]
            # Roll the per-site gates up, keeping provenance. A gate on its own
            # is close to useless -- "if (compactable)" means nothing without
            # the method it came from.
            gates = OrderedDict()
            for site in data["readSites"]:
                for gate in site["gates"]:
                    where = "%s.%s (%s:%d)" % (
                        site["class"] or "?", site["method"] or "?",
                        site["file"], site["line"])
                    gates.setdefault(gate, where)
            entry["gatingConditions"] = [
                OrderedDict([("condition", gate), ("at", where)])
                for gate, where in list(gates.items())[:12]]
        else:
            entry["readSites"] = []
            entry["coConfigs"] = []
            entry["gatingConditions"] = []
            entry["parseWarnings"].append("no read site resolved for this config")
        entry["gatingHeuristic"] = (
            "Conditions are recovered by text matching on enclosing if/switch blocks. "
            "They indicate where a read is conditional; they are not a complete or "
            "verified account of when the config takes effect. Confirm at the cited line.")
        configs.append(entry)

    for entry in keyless:
        entry["module"] = module_of(entry["declaredIn"]["file"])
        entry["readSites"] = []
        entry["coConfigs"] = []
        entry["gatingConditions"] = []
        entry["accessors"] = []
        entry["aliasConstants"] = []
        configs.append(entry)

    stats = {
        "declarationsSeen": decl_stats["declarationsSeen"],
        "distinctKeys": len(by_key),
        "keylessDeclarations": len(keyless),
        "aliasesResolved": decl_stats["aliasesResolved"],
        "aliasesUnresolved": decl_stats["aliasesUnresolved"],
        "flinkOptionsSkipped": decl_stats["flinkOptionsSkipped"],
        "accessorsResolved": len(accessors),
        "readSites": read_stats["readSites"],
        "readSitesCapped": read_stats["readSitesCapped"],
    }
    return configs, stats


def write_summary(configs, stats, catalog, out_path):
    by_group = defaultdict(int)
    by_module = defaultdict(int)
    advanced = 0
    deprecated = 0
    documented = 0
    with_reads = 0
    with_gates = 0
    for entry in configs:
        by_group[entry.get("configGroup") or "(unknown)"] += 1
        by_module[entry.get("module") or "(unknown)"] += 1
        advanced += 1 if entry.get("advanced") else 0
        deprecated += 1 if entry.get("deprecatedAfter") else 0
        documented += 1 if entry.get("documentation") else 0
        with_reads += 1 if entry.get("readSites") else 0
        with_gates += 1 if entry.get("gatingConditions") else 0

    lines = []
    lines.append("<!--")
    lines.append("Licensed to the Apache Software Foundation (ASF) under one")
    lines.append("or more contributor license agreements.  See the NOTICE file")
    lines.append("distributed with this work for additional information")
    lines.append("regarding copyright ownership.  The ASF licenses this file")
    lines.append("to you under the Apache License, Version 2.0 (the")
    lines.append('"License"); you may not use this file except in compliance')
    lines.append("with the License.  You may obtain a copy of the License at")
    lines.append("")
    lines.append("   http://www.apache.org/licenses/LICENSE-2.0")
    lines.append("")
    lines.append("Unless required by applicable law or agreed to in writing, software")
    lines.append('distributed under the License is distributed on an "AS IS" BASIS,')
    lines.append("WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.")
    lines.append("See the License for the specific language governing permissions and")
    lines.append("limitations under the License.")
    lines.append("-->")
    lines.append("")
    lines.append("# Config catalog summary")
    lines.append("")
    lines.append("Generated by `scripts/generate_config_catalog.py`. Do not edit by hand.")
    lines.append("")
    lines.append("| | |")
    lines.append("|---|---|")
    lines.append("| Hudi version | `%s` |" % catalog["hudiVersion"])
    lines.append("| Source commit | `%s` |" % catalog["generatedFrom"])
    lines.append("| Distinct config keys | %d |" % len(configs))
    lines.append("| Advanced | %d |" % advanced)
    lines.append("| Deprecated | %d |" % deprecated)
    lines.append("| With documentation text | %d |" % documented)
    lines.append("| With at least one read site | %d |" % with_reads)
    lines.append("| With at least one gating condition | %d |" % with_gates)
    lines.append("| Read sites recorded | %d |" % stats["readSites"])
    lines.append("| Accessor methods resolved | %d |" % stats["accessorsResolved"])
    lines.append("")
    lines.append("## By module")
    lines.append("")
    lines.append("| Module | Configs |")
    lines.append("|---|---|")
    for module, count in sorted(by_module.items(), key=lambda kv: (-kv[1], kv[0])):
        lines.append("| `%s` | %d |" % (module, count))
    lines.append("")
    lines.append("## By config group")
    lines.append("")
    lines.append("| Group | Configs |")
    lines.append("|---|---|")
    for group, count in sorted(by_group.items(), key=lambda kv: (-kv[1], kv[0])):
        lines.append("| `%s` | %d |" % (group, count))
    lines.append("")

    with open(out_path, "w", encoding="utf-8") as handle:
        handle.write("\n".join(lines))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--repo-root",
        default=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        help="Hudi checkout to scan (default: the repo this script lives in)")
    parser.add_argument(
        "--out-dir", default=None,
        help="Where to write config-catalog.json and config-catalog-summary.md "
             "(default: hudi-agent-gateway/skills/hudi-config-consultant)")
    parser.add_argument("--verbose", action="store_true", help="Per-pass progress on stderr")
    args = parser.parse_args(argv)

    repo_root = os.path.abspath(args.repo_root)
    out_dir = args.out_dir or os.path.join(
        repo_root, "hudi-agent-gateway", "skills", "hudi-config-consultant")
    out_dir = os.path.abspath(out_dir)
    os.makedirs(out_dir, exist_ok=True)

    if args.verbose:
        print("Scanning %s" % repo_root, file=sys.stderr)
    configs, stats = build_catalog(repo_root, args.verbose)

    catalog = OrderedDict([
        ("schemaVersion", 1),
        ("hudiVersion", read_project_version(repo_root)),
        ("generatedFrom", read_git_sha(repo_root)),
        ("configs", configs),
    ])

    json_path = os.path.join(out_dir, "config-catalog.json")
    with open(json_path, "w", encoding="utf-8") as handle:
        json.dump(catalog, handle, indent=2, sort_keys=False, ensure_ascii=False)
        handle.write("\n")

    summary_path = os.path.join(out_dir, "config-catalog-summary.md")
    write_summary(configs, stats, catalog, summary_path)

    fully = sum(1 for c in configs if not c["parseWarnings"])
    partially = len(configs) - fully
    with_reads = sum(1 for c in configs if c["readSites"])
    with_co = sum(1 for c in configs if c["coConfigs"])
    with_gates = sum(1 for c in configs if c["gatingConditions"])

    print("Hudi version      : %s" % catalog["hudiVersion"])
    print("Source commit     : %s" % catalog["generatedFrom"])
    print("Declarations seen : %d" % stats["declarationsSeen"])
    print("Distinct configs  : %d" % len(configs))
    print("  fully parsed    : %d" % fully)
    print("  with warnings   : %d" % partially)
    print("Aliases resolved  : %d (unresolved %d)"
          % (stats["aliasesResolved"], stats["aliasesUnresolved"]))
    print("Accessors         : %d" % stats["accessorsResolved"])
    print("Read sites        : %d across %d configs (%d configs capped at %d)"
          % (stats["readSites"], with_reads, stats["readSitesCapped"], MAX_READ_SITES))
    print("Co-configs        : %d configs have at least one" % with_co)
    print("Gating conditions : %d configs have at least one" % with_gates)
    print("Wrote %s" % json_path)
    print("Wrote %s" % summary_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
