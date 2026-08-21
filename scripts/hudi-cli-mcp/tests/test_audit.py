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

"""Tests for the append-only write-surface audit trail."""

import json
from unittest.mock import MagicMock, patch

import pytest

from hudi_cli_mcp.audit import audit_event, audit_log_path
from hudi_cli_mcp.commands import RiskLevel
from hudi_cli_mcp.executor import ExecutionResult
from hudi_cli_mcp.parser import ParsedOutput
from hudi_cli_mcp.safety import SafetyManager, TokenNotFoundError
from hudi_cli_mcp.session import SessionManager
from hudi_cli_mcp.tools.confirmation import confirm_operation


@pytest.fixture()
def audit_file(tmp_path, monkeypatch):
    path = tmp_path / "audit.log"
    monkeypatch.setenv("HUDI_MCP_AUDIT_LOG", str(path))
    return path


def _events(path):
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines()]


def _mock_executor():
    executor = MagicMock()
    executor.execute.return_value = ExecutionResult(
        raw_output="ok",
        parsed=ParsedOutput(),
        return_code=0,
        duration_seconds=1.5,
    )
    return executor


class TestSinkResolution:
    def test_default_is_home_dotdir(self, monkeypatch):
        monkeypatch.delenv("HUDI_MCP_AUDIT_LOG", raising=False)
        assert audit_log_path().endswith("/.hudi-mcp/audit.log")

    def test_off_disables(self, monkeypatch):
        for value in ("off", "OFF", "none", "0", "false"):
            monkeypatch.setenv("HUDI_MCP_AUDIT_LOG", value)
            assert audit_log_path() is None

    def test_custom_path(self, monkeypatch):
        monkeypatch.setenv("HUDI_MCP_AUDIT_LOG", "/var/log/hudi-audit.jsonl")
        assert audit_log_path() == "/var/log/hudi-audit.jsonl"


class TestAuditEvent:
    def test_writes_json_line(self, audit_file):
        audit_event("prepare", command="cleans run", risk="high")
        (entry,) = _events(audit_file)
        assert entry["event"] == "prepare"
        assert entry["command"] == "cleans run"
        assert "ts" in entry

    def test_none_fields_dropped(self, audit_file):
        audit_event("execute", command="x", error=None)
        (entry,) = _events(audit_file)
        assert "error" not in entry

    def test_disabled_writes_nothing(self, tmp_path, monkeypatch):
        monkeypatch.setenv("HUDI_MCP_AUDIT_LOG", "off")
        audit_event("prepare", command="x")
        assert list(tmp_path.iterdir()) == []

    def test_unwritable_sink_never_raises(self, monkeypatch):
        # A path under a file (not a dir) cannot be created -- must be swallowed.
        monkeypatch.setenv("HUDI_MCP_AUDIT_LOG", "/dev/null/nope/audit.log")
        audit_event("prepare", command="x")  # no exception

    def test_creates_parent_directory(self, tmp_path, monkeypatch):
        path = tmp_path / "nested" / "dir" / "audit.log"
        monkeypatch.setenv("HUDI_MCP_AUDIT_LOG", str(path))
        audit_event("prepare", command="x")
        assert path.exists()


class TestLifecycleEvents:
    def test_prepare_confirm_execute_flow(self, audit_file):
        safety = SafetyManager()
        session = SessionManager()
        session.connect("/tmp/table")
        op = safety.prepare_operation(
            command="compaction run",
            risk_level=RiskLevel.HIGH,
            table_path="/tmp/table",
            description="d",
        )
        confirm_operation(op.token, _mock_executor(), session, safety)
        names = [e["event"] for e in _events(audit_file)]
        assert names == ["prepare", "confirm", "execute"]
        execute = _events(audit_file)[-1]
        assert execute["success"] is True
        assert execute["duration_seconds"] == 1.5
        assert execute["table_path"] == "/tmp/table"

    def test_full_token_never_logged(self, audit_file):
        safety = SafetyManager()
        op = safety.prepare_operation(
            command="cleans run",
            risk_level=RiskLevel.HIGH,
            table_path="/tmp/table",
            description="d",
        )
        content = audit_file.read_text()
        assert op.token not in content
        assert op.token[:8] in content

    def test_cancel_and_dedupe_events(self, audit_file):
        safety = SafetyManager()
        op = safety.prepare_operation(
            command="cleans run",
            risk_level=RiskLevel.HIGH,
            table_path="/tmp/table",
            description="d",
        )
        safety.prepare_operation(  # identical -> dedupe
            command="cleans run",
            risk_level=RiskLevel.HIGH,
            table_path="/tmp/table",
            description="d",
        )
        safety.cancel(op.token)
        names = [e["event"] for e in _events(audit_file)]
        assert names == ["prepare", "prepare_deduped", "cancel"]

    def test_confirm_rejected_logged(self, audit_file):
        safety = SafetyManager()
        with pytest.raises(TokenNotFoundError):
            safety.confirm("bogus-token-1234")
        (entry,) = _events(audit_file)
        assert entry["event"] == "confirm_rejected"
        assert entry["reason"] == "not found"
        assert entry["token"] == "bogus-to"

    def test_expire_logged(self, audit_file):
        safety = SafetyManager()
        op = safety.prepare_operation(
            command="cleans run",
            risk_level=RiskLevel.HIGH,
            table_path="/tmp/table",
            description="d",
        )
        with patch("hudi_cli_mcp.safety.time.time", return_value=op.created_at + 400):
            safety.list_pending()
        names = [e["event"] for e in _events(audit_file)]
        assert names == ["prepare", "expire"]
