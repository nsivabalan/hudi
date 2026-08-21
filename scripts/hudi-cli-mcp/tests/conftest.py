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

import pytest


@pytest.fixture(autouse=True)
def _no_audit_log_in_tests(monkeypatch):
    """Auditing defaults to ~/.hudi-mcp/audit.log; the suite exercises the write
    surface heavily and must not write into the developer's home directory.
    Tests that assert on auditing re-point HUDI_MCP_AUDIT_LOG themselves."""
    monkeypatch.setenv("HUDI_MCP_AUDIT_LOG", "off")
