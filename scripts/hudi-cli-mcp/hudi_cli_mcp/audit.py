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

"""Append-only audit trail for the write surface.

Every write-operation lifecycle event (prepare, confirm, cancel, expire,
execute) is appended as one JSON line, so an operator can answer "what did
the assistant do to my table, and when" after the fact. Read commands are
deliberately not logged.

Sink selection via the ``HUDI_MCP_AUDIT_LOG`` environment variable:
  - unset or empty  -> ``~/.hudi-mcp/audit.log`` (auditing is on by default;
    an opt-in audit log is not there when it is actually needed)
  - ``off``         -> disabled
  - any other value -> used as the log file path

Writing is best-effort: an unwritable sink must never block or fail the
operation being audited.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone

AUDIT_LOG_ENV = "HUDI_MCP_AUDIT_LOG"

_DISABLED_VALUES = {"off", "none", "disabled", "false", "0"}


def audit_log_path() -> str | None:
    """Resolve the audit sink from the environment, or None when disabled."""
    value = os.environ.get(AUDIT_LOG_ENV, "").strip()
    if value.lower() in _DISABLED_VALUES:
        return None
    return value or os.path.join(os.path.expanduser("~"), ".hudi-mcp", "audit.log")


def short_token(token: str | None) -> str | None:
    """First 8 characters only -- a live confirmation token must never be
    recoverable from the audit log."""
    return token[:8] if token else None


def audit_event(event: str, **fields) -> None:
    """Append one audit event as a JSON line. Never raises."""
    path = audit_log_path()
    if not path:
        return
    entry: dict = {
        "ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "event": event,
    }
    entry.update({k: v for k, v in fields.items() if v is not None})
    try:
        parent = os.path.dirname(path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        # One write() of one line in append mode: atomic enough on POSIX for
        # line-oriented logs at CLI cadence (each op takes seconds anyway).
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, default=str) + "\n")
    except OSError:
        pass  # best-effort: auditing must not take down the operation itself
