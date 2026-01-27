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

package org.apache.hudi.common.util;

import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.DefaultHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieAvroRecordMerger;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.exception.HoodieException;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestHoodieRecordUtils {

  @Test
  void loadHoodieMerge() {
    String mergeClassName = HoodieAvroRecordMerger.class.getName();
    HoodieRecordMerger recordMerger1 = HoodieRecordUtils.loadRecordMerger(mergeClassName);
    HoodieRecordMerger recordMerger2 = HoodieRecordUtils.loadRecordMerger(mergeClassName);
    assertEquals(recordMerger1.getClass().getName(), mergeClassName);
    assertEquals(recordMerger2.getClass().getName(), mergeClassName);
  }

  @Test
  void loadHoodieMergeWithWrongMerger() {
    String mergeClassName = "wrong.package.MergerName";
    assertThrows(HoodieException.class, () -> HoodieRecordUtils.loadRecordMerger(mergeClassName));
  }

  @Test
  void loadPayload() {
    String payloadClassName = DefaultHoodieRecordPayload.class.getName();
    HoodieRecordPayload payload = HoodieRecordUtils.loadPayload(payloadClassName, null, 0);
    assertEquals(payload.getClass().getName(), payloadClassName);
  }

  @Test
  void testGetOrderingFields() {
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    TypedProperties props = new TypedProperties();
    // Assert empty ordering fields for commit time ordering
    assertTrue(HoodieRecordUtils.getOrderingFieldNames(RecordMergeMode.COMMIT_TIME_ORDERING, metaClient).isEmpty());

    // Assert table config precombine fields are returned when props are not set with event time merge mode
    HoodieTableConfig tableConfig = new HoodieTableConfig();
    tableConfig.setValue(HoodieTableConfig.ORDERING_FIELDS, "tbl");
    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    assertEquals(Collections.singletonList("tbl"), HoodieRecordUtils.getOrderingFieldNames(RecordMergeMode.EVENT_TIME_ORDERING, metaClient));

    // Assert table config's ordering value is still returned even when props are set to another value
    props.setProperty("hoodie.table.ordering.fields", "props");
    assertEquals(Collections.singletonList("tbl"), HoodieRecordUtils.getOrderingFieldNames(RecordMergeMode.EVENT_TIME_ORDERING, metaClient));
  }

  @Test
  void testResolveRecordKeyWithNonNullRecordKey() {
    // Test with string record key
    String recordKey = "test_record_key_123";
    String dataFilePath = "partition/fileId";
    long rowPosition = 42L;

    String result = HoodieRecordUtils.resolveRecordKey(recordKey, dataFilePath, rowPosition);
    assertEquals(recordKey, result, "Should return the record key as-is when it's not null");
  }

  @Test
  void testResolveRecordKeyWithNonNullNumericRecordKey() {
    // Test with numeric record key
    Long recordKey = 12345L;
    String dataFilePath = "partition/fileId";
    long rowPosition = 42L;

    String result = HoodieRecordUtils.resolveRecordKey(recordKey, dataFilePath, rowPosition);
    assertEquals("12345", result, "Should convert numeric record key to string");
  }

  @Test
  void testResolveRecordKeyWithNullRecordKey() {
    // Test with null record key - should use fallback pattern
    String dataFilePath = "partition/fileId";
    long rowPosition = 42L;
    String result = HoodieRecordUtils.resolveRecordKey(null, dataFilePath, rowPosition);
    assertEquals("partition/fileId_42", result,
        "Should return fallback pattern 'dataFilePath_rowPosition' when record key is null");
  }

  @Test
  void testResolveRecordKeyWithNullRecordKeyAndZeroPosition() {
    // Test with null record key and zero row position
    String dataFilePath = "partition/fileId";
    long rowPosition = 0L;

    String result = HoodieRecordUtils.resolveRecordKey(null, dataFilePath, rowPosition);
    assertEquals("partition/fileId_0", result,
        "Should handle zero row position correctly in fallback pattern");
  }

  @Test
  void testResolveRecordKeyWithNullRecordKeyAndLargePosition() {
    // Test with null record key and large row position
    String dataFilePath = "partition/external_file.parquet";
    long rowPosition = 999999999L;
    String result = HoodieRecordUtils.resolveRecordKey(null, dataFilePath, rowPosition);
    assertEquals("partition/external_file.parquet_999999999", result,
        "Should handle large row position correctly in fallback pattern");
  }

  @Test
  void testResolveRecordKeyWithComplexDataFilePath() {
    // Test with complex nested partition path
    String dataFilePath = "year=2023/month=12/day=31/part-00000-abc123.parquet";
    long rowPosition = 10L;

    // With non-null record key
    String result1 = HoodieRecordUtils.resolveRecordKey("key123", dataFilePath, rowPosition);
    assertEquals("key123", result1, "Should return record key regardless of complex path");

    // With null record key
    String result2 = HoodieRecordUtils.resolveRecordKey(null, dataFilePath, rowPosition);
    assertEquals("year=2023/month=12/day=31/part-00000-abc123.parquet_10", result2,
        "Should handle complex file paths in fallback pattern");
  }

  @Test
  void testResolveRecordKeyWithRelativePath() {
    // Test with relative path (typical for external files)
    String dataFilePath = "relative/path/to/file";
    long rowPosition = 5L;

    String result = HoodieRecordUtils.resolveRecordKey(null, dataFilePath, rowPosition);
    assertEquals("relative/path/to/file_5", result,
        "Should handle relative paths correctly in fallback pattern");
  }

  @Test
  void testResolveRecordKeyWithEmptyStringRecordKey() {
    // Test with empty string record key (not null, but empty)
    String recordKey = "";
    String dataFilePath = "partition/fileId";
    long rowPosition = 1L;

    String result = HoodieRecordUtils.resolveRecordKey(recordKey, dataFilePath, rowPosition);
    assertEquals("", result, "Should return empty string when record key is empty string (not null)");
  }

  @Test
  void testResolveRecordKeyConsistency() {
    // Test that the same inputs produce the same output (idempotency)
    String dataFilePath = "partition/fileId";
    long rowPosition = 100L;

    String result1 = HoodieRecordUtils.resolveRecordKey(null, dataFilePath, rowPosition);
    String result2 = HoodieRecordUtils.resolveRecordKey(null, dataFilePath, rowPosition);

    assertEquals(result1, result2, "Same inputs should produce identical fallback keys");
    assertEquals("partition/fileId_100", result1, "Fallback key should be consistent");
  }
}