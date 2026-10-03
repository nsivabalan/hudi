<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Catalog schema and how to query it

## Envelope

```json
{
  "schemaVersion": 1,
  "hudiVersion": "1.3.0-SNAPSHOT",
  "generatedFrom": "<git sha of the tree it was generated from>",
  "configs": [ ... ]
}
```

`configs` is sorted by `key`, so regeneration produces a readable diff. Read `hudiVersion` and
`generatedFrom` on every query; they belong in your answer.

## A config entry

### Declared facts — extracted verbatim from the builder chain

| Field | Meaning |
|---|---|
| `key` | The property key, e.g. `hoodie.compact.inline.max.delta.commits`. |
| `keyResolution` | `literal`, `resolvedFromConstants` (key built from a `static final String` prefix), or `derivedFromConfigKey` (key built from another config's key). |
| `keyExpression` | The source expression, when the key was not a plain literal. |
| `type` | The declared type parameter: `String`, `Boolean`, `Integer`, `Long`, … |
| `declaredIn` | `{class, constant, file, line}` — where the `ConfigProperty` is declared. |
| `configGroup` | The owning config class, e.g. `HoodieCompactionConfig`. Use this to group related configs. |
| `module` | Top-level Maven module, e.g. `hudi-client`. |
| `hasDefaultValue` | `false` for `noDefaultValue()` configs — these are required or inferred. |
| `defaultValue` | The default, or `null`. |
| `defaultValueIsLiteral` | `false` means `defaultValue` is a **source expression**, not a value — e.g. `String.valueOf(104857600)` or `KEEP_LATEST_COMMITS.name()`. |
| `defaultValueNote` | Present when the default is computed, to say so. |
| `documentation` | The `withDocumentation` text. |
| `documentationSource` | `literal`, `resolvedFromConstants`, or `enumClass:<Name>` when the docs live on an enum. |
| `enumDocumentation` | For `enumClass` docs: `{enum, file, description, values: [{value, description}]}`. **This is where valid values and their meanings live** for strategy-style configs. |
| `validValues` | From `withValidValues`, when present. Often empty even for enum-valued configs — check `enumDocumentation` too. |
| `alternatives` | From `withAlternatives`: older key names that still work. |
| `sinceVersion`, `deprecatedAfter`, `supportedVersions` | Version metadata, when declared. |
| `advanced` | `markAdvanced()` was called. A tuning knob, not a routine setting. |
| `hasInferFunction` | Hudi may derive this value from other configs when unset. |
| `builderSteps` | The builder methods seen, in order. Useful for spotting what the parser did and did not see. |
| `parseWarnings` | Non-empty means the generator could not fully read something. Check before answering. |
| `aliasConstants` | Other Java constants that re-export the same config. |
| `alsoDeclaredIn` | Present when the same key is declared in more than one class. |

### Derived context — recovered from code, heuristic

| Field | Meaning |
|---|---|
| `accessors` | Config-class methods that read this config and nothing else, e.g. `HoodieWriteConfig.getInlineCompactDeltaCommitMax`. These are what consumers actually call. |
| `readSites` | Up to 20 `{class, method, file, line, via, gates}`. `via` is `constant` (the config constant is named at that line) or `accessor:<name>` (the line calls a resolved accessor). |
| `coConfigs` | Up to 25 `{key, sharedMethods}` — other configs mentioned in the same method bodies. A **co-occurrence** measure, not a declared dependency. |
| `gatingConditions` | Up to 12 `{condition, at}` — enclosing `if`/`switch` conditions, with the `class.method (file:line)` they came from. |
| `gatingHeuristic` | The standing caveat. Repeat its substance when you quote a gate. |

## Reading `readSites` well

Not all read sites are equally interesting:

- `via: "accessor:..."` in a class like `ScheduleCompactionActionExecutor` or `TimelineArchiverV2`
  is a **real consumer** — code that acts on the value. Lead with these.
- `via: "constant"` inside the owning config class (a `withX(...)` builder method, or the accessor
  itself) is **plumbing**. Mention it only to show where the accessor is defined.
- `via: "constant"` in `hudi-cli` is a tool, not the write path.
- `method: null` with `via: "scalaReference"` is a Scala file; the generator does not parse Scala
  method bodies, so there is no method name and no gating for these.

When `readSites` has exactly 20 entries the list was capped — there may be more consumers.

## Query recipes

All of these assume you are in the skill directory.

**One config, everything:**

```bash
python3 - <<'EOF'
import json
catalog = json.load(open('config-catalog.json'))
by_key = {c['key']: c for c in catalog['configs']}
entry = by_key.get('hoodie.compact.inline.max.delta.commits')
print(json.dumps(entry, indent=2) if entry else 'not in catalog')
EOF
```

**Find by substring, in key or documentation:**

```bash
python3 - <<'EOF'
import json
term = 'small file'
for c in json.load(open('config-catalog.json'))['configs']:
    haystack = (c['key'] + ' ' + (c['documentation'] or '')).lower()
    if term in haystack:
        print(c['key'], '|', c['defaultValue'], '| advanced', c['advanced'])
EOF
```

**Is this key an alternative of something else?**

```bash
python3 - <<'EOF'
import json
needle = 'hoodie.cleaner.policy'
for c in json.load(open('config-catalog.json'))['configs']:
    if needle in c['alternatives']:
        print(needle, 'is an old alias of', c['key'])
EOF
```

**The gating story for a config:**

```bash
python3 - <<'EOF'
import json
by_key = {c['key']: c for c in json.load(open('config-catalog.json'))['configs']}
entry = by_key['hoodie.compact.inline.max.delta.commits']
for gate in entry['gatingConditions']:
    print('-', gate['condition'])
    print('   at', gate['at'])
EOF
```

**Everything in one config group:**

```bash
jq -r '.configs[] | select(.configGroup == "HoodieCleanConfig")
       | "\(.key)\t\(.defaultValue)\t advanced=\(.advanced)"' config-catalog.json
```

**Deprecated configs:**

```bash
jq -r '.configs[] | select(.deprecatedAfter != null)
       | "\(.key)\tdeprecated after \(.deprecatedAfter)"' config-catalog.json
```

**Configs with no resolved consumer** (a catalog gap, not proof the config is dead):

```bash
jq -r '.configs[] | select(.readSites | length == 0) | .key' config-catalog.json
```

## Worked example

`hoodie.compact.inline.max.delta.commits` is the case that motivates the whole catalog.

The published reference gives: key, default `5`, and a description. The catalog adds, from code:

- `accessors`: `HoodieWriteConfig.getInlineCompactDeltaCommitMax` at
  `HoodieWriteConfig.java:2031`.
- A real consumer: `ScheduleCompactionActionExecutor.needCompact` at
  `ScheduleCompactionActionExecutor.java:208`, `via: accessor:getInlineCompactDeltaCommitMax`.
- Its gates at that site: `switch (compactionTriggerStrategy)` under `case NUM_COMMITS`,
  `NUM_COMMITS_AFTER_LAST_REQUEST`, `NUM_OR_TIME` and `NUM_AND_TIME`.
- Top `coConfig`: `hoodie.compact.inline.trigger.strategy`, sharing 4 read-site methods.

So the answer to "I set this to 20 and compaction still triggers every 5 commits" is visible in the
catalog: the value is only consulted under four of the trigger strategies, and the strategy is set
by the co-config. That conclusion is independently confirmed by the config's own documentation —
which is a good sign that the heuristic is working, and a reminder to cross-check it against the
documentation whenever both are present.
