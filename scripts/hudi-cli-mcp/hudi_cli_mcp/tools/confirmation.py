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

"""Confirmation tools for pending write operations."""

from __future__ import annotations

import json

from hudi_cli_mcp.audit import audit_event, short_token
from hudi_cli_mcp.commands import quirk_hint, quote_arg
from hudi_cli_mcp.executor import WRITE_TIMEOUT, ExecutionResult, HudiCliExecutor
from hudi_cli_mcp.safety import SafetyManager, TokenExpiredError, TokenNotFoundError
from hudi_cli_mcp.session import SessionManager

# Plan-scheduling commands whose CLI output cannot be trusted either way: a
# successful `compaction schedule` prints "Failed to run compaction", and a
# schedule that silently does nothing (e.g. a pending plan already covers the
# eligible file groups) leaves a pre-existing REQUESTED instant that LOOKS like
# confirmation. For these, the schedule is bracketed with the plan-listing
# command in the SAME CLI session and success is decided by diffing the plan
# instants before/after — never by the message text.
_PLAN_LISTINGS: dict[str, tuple[str, str]] = {
    "compaction schedule": ("compactions show all", "Compaction Instant Time"),
    "compaction scheduleandexecute": ("compactions show all", "Compaction Instant Time"),
}


def _plan_listing_for(command: str) -> tuple[str, str] | None:
    lowered = " ".join(command.strip().lower().split())
    for prefix, listing in _PLAN_LISTINGS.items():
        if lowered == prefix or lowered.startswith(prefix + " "):
            return listing
    return None


def _verify_plan_created(result: ExecutionResult, instant_header: str) -> dict | None:
    """Diff plan instants between the first and last plan listing in ``result``.

    Returns a verification payload, or None when fewer than two listings parsed
    (inconclusive — fall back to the raw result's semantics).
    """
    listings = [t for t in result.parsed.tables if instant_header in t.headers]
    if len(listings) < 2:
        return None

    def instants(table) -> set[str]:
        return {(r.get(instant_header) or "").strip() for r in table.rows} - {""}

    created = sorted(instants(listings[-1]) - instants(listings[0]))
    if created:
        return {
            "plan_created": True,
            "created_instants": created,
            "note": "Verified by diffing the plan listing before/after in the same "
            "CLI session. Any 'Failed to run compaction' text is cosmetic when a "
            "new plan instant appears.",
        }
    pending = sorted(
        (r.get(instant_header) or "").strip()
        for r in listings[-1].rows
        if (r.get("State") or "").strip().upper() == "REQUESTED"
    )
    return {
        "plan_created": False,
        "created_instants": [],
        "pre_existing_pending_plans": pending,
        "note": "No new plan instant appeared after the schedule — the operation "
        "did NOT take effect. Every REQUESTED instant listed already existed "
        "before this call; do not attribute it to this schedule. Common causes: "
        "an existing pending plan already covers the eligible file groups, or "
        "there is no new data to compact.",
    }


def confirm_operation(
    token: str,
    executor: HudiCliExecutor,
    session: SessionManager,
    safety: SafetyManager,
) -> str:
    """Confirm and execute a pending write operation."""
    try:
        op = safety.confirm(token)
    except (TokenNotFoundError, TokenExpiredError) as e:
        return json.dumps({"success": False, "error": str(e)}, indent=2)

    # Execute the confirmed command. Write operations launch real Spark jobs
    # (compaction, clustering, rollback) that routinely take many minutes, so
    # they get the write-path timeout, not the 120s read default.
    listing = _plan_listing_for(op.command)
    commands = [f"connect --path {quote_arg(op.table_path)}"]
    if listing:
        commands.extend([listing[0], op.command, listing[0]])
    else:
        commands.append(op.command)
    result = executor.execute(commands, timeout=WRITE_TIMEOUT)

    success = result.is_success()
    error = result.parsed.errors[0] if result.parsed.errors else None
    verification = _verify_plan_created(result, listing[1]) if listing else None
    if verification is not None:
        success = verification["plan_created"]
        error = None if success else f"'{op.command}' did not create a new plan instant"

    audit_event(
        "execute",
        command=op.command,
        table_path=op.table_path,
        risk=op.risk_level.value,
        token=short_token(token),
        success=success,
        duration_seconds=round(result.duration_seconds, 2),
        error=error,
    )

    output = result.to_dict()
    output["success"] = success
    output["confirmed_command"] = op.command
    output["risk_level"] = op.risk_level.value
    output["table_path"] = op.table_path
    if verification is not None:
        # The before/after diff supersedes the text-based quirk hint.
        output["plan_verification"] = verification
        if error:
            output["error"] = error
    else:
        hint = quirk_hint(op.command)
        if hint:
            output["hint"] = hint
    return json.dumps(output, indent=2)


def cancel_operation(
    token: str,
    safety: SafetyManager,
) -> str:
    """Cancel a pending write operation."""
    try:
        op = safety.cancel(token)
    except TokenNotFoundError as e:
        return json.dumps({"success": False, "error": str(e)}, indent=2)

    return json.dumps(
        {
            "success": True,
            "message": "Operation cancelled.",
            "cancelled_command": op.command,
        },
        indent=2,
    )


def list_pending_operations(safety: SafetyManager) -> str:
    """List all pending write operations awaiting confirmation."""
    pending = safety.list_pending()
    return json.dumps(
        {
            "success": True,
            "pending_count": len(pending),
            "operations": [op.to_dict() for op in pending],
        },
        indent=2,
    )
