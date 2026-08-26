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

"""Tests for confirm-time plan verification on schedule operations.

Encodes a real failure observed live: `compaction schedule` genuinely failed
(a pending plan already covered the eligible file groups), but the text-based
quirk hint told the model to dismiss the 'Failed to run compaction' error, and
a PRE-EXISTING requested plan in `compactions show all` looked like proof of
success. Verification diffs the plan listing before/after in the same CLI
session so neither the bogus failure text nor a pre-existing plan can mislead.
"""

import json
from unittest.mock import MagicMock

from hudi_cli_mcp.commands import RiskLevel
from hudi_cli_mcp.executor import ExecutionResult
from hudi_cli_mcp.parser import ParsedOutput, ParsedTable
from hudi_cli_mcp.safety import SafetyManager
from hudi_cli_mcp.session import SessionManager
from hudi_cli_mcp.tools.confirmation import confirm_operation

_HEADER = "Compaction Instant Time"


def _plan_table(instants_and_states):
    return ParsedTable(
        headers=[_HEADER, "State", "Total FileIds to be Compacted"],
        rows=[
            {_HEADER: instant, "State": state, "Total FileIds to be Compacted": "2"}
            for instant, state in instants_and_states
        ],
    )


def _executor(tables, errors=None):
    executor = MagicMock()
    executor.execute.return_value = ExecutionResult(
        raw_output="",
        parsed=ParsedOutput(tables=tables, errors=errors or []),
        return_code=0,
        duration_seconds=0.1,
    )
    return executor


def _prepare(safety, command="compaction schedule", risk=RiskLevel.MEDIUM):
    return safety.prepare_operation(
        command=command,
        risk_level=risk,
        table_path="/tmp/table",
        description="d",
    )


def _confirm(executor, command="compaction schedule", risk=RiskLevel.MEDIUM):
    session = SessionManager()
    session.connect("/tmp/table")
    safety = SafetyManager()
    op = _prepare(safety, command, risk)
    return json.loads(confirm_operation(op.token, executor, session, safety)), executor


class TestScheduleVerification:
    def test_new_plan_overrides_bogus_failure_text(self):
        # The CLI printed 'Failed to run compaction' but a new instant appeared:
        # verification must declare success and name the created instant.
        executor = _executor(
            tables=[
                _plan_table([("100", "COMPLETED")]),
                _plan_table([("100", "COMPLETED"), ("200", "REQUESTED")]),
            ],
            errors=["Failed to run compaction for trips_demo"],
        )
        result, _ = _confirm(executor)
        assert result["success"] is True
        assert result["plan_verification"]["plan_created"] is True
        assert result["plan_verification"]["created_instants"] == ["200"]

    def test_pre_existing_plan_is_not_success(self):
        # The exact live failure: no new instant, but a pending plan from an
        # earlier schedule sits in REQUESTED state. Must NOT be attributed to
        # this call.
        before = [("100", "COMPLETED"), ("150", "REQUESTED")]
        executor = _executor(
            tables=[_plan_table(before), _plan_table(before)],
            errors=["Failed to run compaction for trips_demo"],
        )
        result, _ = _confirm(executor)
        assert result["success"] is False
        assert "did not create a new plan instant" in result["error"]
        v = result["plan_verification"]
        assert v["plan_created"] is False
        assert v["pre_existing_pending_plans"] == ["150"]

    def test_no_new_plan_fails_even_without_error_text(self):
        before = [("100", "COMPLETED")]
        executor = _executor(tables=[_plan_table(before), _plan_table(before)])
        result, _ = _confirm(executor)
        assert result["success"] is False

    def test_schedule_brackets_command_with_plan_listing(self):
        executor = _executor(tables=[_plan_table([]), _plan_table([])])
        _confirm(executor)
        (commands,), _ = executor.execute.call_args
        assert commands == [
            'connect --path /tmp/table',
            "compactions show all",
            "compaction schedule",
            "compactions show all",
        ]

    def test_schedule_and_execute_also_verified(self):
        executor = _executor(
            tables=[
                _plan_table([("100", "COMPLETED")]),
                _plan_table([("100", "COMPLETED"), ("300", "COMPLETED")]),
            ],
            errors=["Failed to run compaction for trips_demo"],
        )
        result, _ = _confirm(
            executor, command="compaction scheduleAndExecute", risk=RiskLevel.HIGH
        )
        assert result["success"] is True
        assert result["plan_verification"]["created_instants"] == ["300"]

    def test_inconclusive_listing_falls_back_to_raw_semantics(self):
        # Only one plan listing parsed (e.g. the before-listing failed): no
        # verification claim either way; the raw error keeps failing the op.
        executor = _executor(
            tables=[_plan_table([("100", "COMPLETED")])],
            errors=["Failed to run compaction for trips_demo"],
        )
        result, _ = _confirm(executor)
        assert result["success"] is False
        assert "plan_verification" not in result

    def test_non_schedule_command_unaffected(self):
        executor = _executor(tables=[])
        result, executor = _confirm(executor, command="compaction run", risk=RiskLevel.HIGH)
        (commands,), _ = executor.execute.call_args
        assert commands == ["connect --path /tmp/table", "compaction run"]
        assert "plan_verification" not in result
        assert result["success"] is True
