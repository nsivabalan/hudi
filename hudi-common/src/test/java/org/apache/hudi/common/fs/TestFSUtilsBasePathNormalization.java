/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.fs;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Pure-logic tests for {@link FSUtils#normalizeBasePathForLocking}. {@code FSUtils} lives in
 * {@code hudi-common}, so this test must also live here for the method's coverage to be attributed
 * to the {@code hudi-common} jacoco report — the more general {@code TestFSUtils} in
 * {@code hudi-hadoop-common} runs in a different CI lane and its coverage is not collected for
 * the {@code hudi-common} source.
 */
class TestFSUtilsBasePathNormalization {

  @Test
  void testNormalizeBasePathForLocking() {
    // Canonical form ends with exactly one trailing slash.
    assertEquals("s3://my-bucket/path/", FSUtils.normalizeBasePathForLocking("s3://my-bucket/path"));
    assertEquals("s3://my-bucket/path/", FSUtils.normalizeBasePathForLocking("s3://my-bucket/path/"));
    // Multiple trailing slashes collapse to one.
    assertEquals("s3://my-bucket/path/", FSUtils.normalizeBasePathForLocking("s3://my-bucket/path///"));
    // s3a:// is normalized to s3:// (delegates to s3aToS3).
    assertEquals("s3://my-bucket/path/", FSUtils.normalizeBasePathForLocking("s3a://my-bucket/path"));
    assertEquals("s3://my-bucket/path/", FSUtils.normalizeBasePathForLocking("S3A://my-bucket/path/"));
    // Whitespace surrounding the path is trimmed.
    assertEquals("s3://my-bucket/path/", FSUtils.normalizeBasePathForLocking("  s3://my-bucket/path  "));
    assertEquals("s3://my-bucket/path/", FSUtils.normalizeBasePathForLocking("\ts3a://my-bucket/path/\n"));
    // Non-S3 schemes pass through (still get trailing-slash normalization).
    assertEquals("gs://my-bucket/path/", FSUtils.normalizeBasePathForLocking("gs://my-bucket/path"));
    assertEquals("gs://my-bucket/path/", FSUtils.normalizeBasePathForLocking("gs://my-bucket/path//"));
    // Inner consecutive slashes are intentionally NOT touched (could be a real S3 key).
    assertEquals("s3://my-bucket//inner/path/", FSUtils.normalizeBasePathForLocking("s3://my-bucket//inner/path"));
    // S3 object keys are allowed to end with ':' — a final-segment colon must NOT be
    // mis-classified as the "scheme-only" case. The trailing ':' is part of the key and
    // is preserved before the single trailing slash is appended.
    assertEquals("s3://my-bucket/foo:/", FSUtils.normalizeBasePathForLocking("s3://my-bucket/foo:"));
    assertEquals("s3://my-bucket/foo:/", FSUtils.normalizeBasePathForLocking("s3://my-bucket/foo:/"));
    assertEquals("s3://my-bucket/foo:bar:/", FSUtils.normalizeBasePathForLocking("s3://my-bucket/foo:bar:/"));
    assertEquals("s3://my-bucket/foo:/", FSUtils.normalizeBasePathForLocking("s3a://my-bucket/foo:///"));
    // Random ASCII chars (URL-unsafe and equals/colon/plus/hash/ampersand/space) pass through
    // unchanged except for the trailing-slash and s3a-scheme rules. Hudi does not re-encode
    // paths internally so the lock key must be byte-stable across these characters.
    assertEquals(
        "s3://my-bucket/datalake/db=foo:bar/dt=2024-01-01T00:00:00+05:30/region=us east/category=a&b=c/vehicle#1/file/",
        FSUtils.normalizeBasePathForLocking(
            "s3a://my-bucket/datalake/db=foo:bar/dt=2024-01-01T00:00:00+05:30/region=us east/category=a&b=c/vehicle#1/file"));
    // Null and empty are rejected.
    assertThrows(IllegalArgumentException.class, () -> FSUtils.normalizeBasePathForLocking(null));
    assertThrows(IllegalArgumentException.class, () -> FSUtils.normalizeBasePathForLocking(""));
    assertThrows(IllegalArgumentException.class, () -> FSUtils.normalizeBasePathForLocking("   "));
    // Scheme-only inputs and all-slash inputs are rejected — stripping leaves nothing
    // meaningful to lock against.
    assertThrows(IllegalArgumentException.class, () -> FSUtils.normalizeBasePathForLocking("s3://"));
    assertThrows(IllegalArgumentException.class, () -> FSUtils.normalizeBasePathForLocking("s3:///"));
    assertThrows(IllegalArgumentException.class, () -> FSUtils.normalizeBasePathForLocking("s3a://"));
    assertThrows(IllegalArgumentException.class, () -> FSUtils.normalizeBasePathForLocking("s3a:///"));
    assertThrows(IllegalArgumentException.class, () -> FSUtils.normalizeBasePathForLocking("///"));
    assertThrows(IllegalArgumentException.class, () -> FSUtils.normalizeBasePathForLocking("/"));
  }
}
