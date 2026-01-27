/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.common.util;

import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.storage.StoragePath;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestExternalFilePathUtil {

  private static final String BASE_PATH = "/tmp/test_table";
  private static final String PARTITION_PATH = "2024/01/01";
  private static final String COMMIT_TIME = "20240101120000";
  // For external files: the external file name format is <originalFileName>_<commitTime>_hudiext
  // When normalized/extracted, it returns the original file name without Hudi commit time format
  private static final String NORMALIZED_FILE_NAME = "file1-0-0-0-1-0.parquet";
  private static final String EXTERNAL_FILE_NAME = NORMALIZED_FILE_NAME + "_" + COMMIT_TIME + "_hudiext";
  private static final long FILE_SIZE = 1024L;

  /**
   * Creates a mock HoodieWriteStat with standard test values.
   */
  private HoodieWriteStat createMockWriteStat() {
    HoodieWriteStat writeStat = new HoodieWriteStat();
    writeStat.setFileId(NORMALIZED_FILE_NAME);
    writeStat.setPartitionPath(PARTITION_PATH);
    writeStat.setPath(PARTITION_PATH + "/" + EXTERNAL_FILE_NAME);
    writeStat.setFileSizeInBytes(FILE_SIZE);
    return writeStat;
  }

  /**
   * Asserts basic FileSlice properties: not null, partition path, file ID, commit time, and base file presence.
   */
  private void assertBasicFileSliceProperties(FileSlice fileSlice, String expectedPartition, String expectedFileId, String expectedCommitTime) {
    assertNotNull(fileSlice);
    assertEquals(expectedPartition, fileSlice.getPartitionPath());
    assertEquals(expectedFileId, fileSlice.getFileId());
    assertEquals(expectedCommitTime, fileSlice.getBaseInstantTime());
    assertTrue(fileSlice.getBaseFile().isPresent());
  }

  /**
   * Asserts that the FileSlice has no log files.
   */
  private void assertNoLogFiles(FileSlice fileSlice) {
    assertEquals(0, fileSlice.getLogFiles().count());
  }

  /**
   * Asserts the base file path matches the expected path.
   */
  private void assertBaseFilePath(FileSlice fileSlice, String expectedPath) {
    assertEquals(expectedPath, fileSlice.getBaseFile().get().getPath());
  }

  /**
   * Asserts the base file size matches the expected size.
   */
  private void assertBaseFileSize(FileSlice fileSlice, long expectedSize) {
    assertEquals(expectedSize, fileSlice.getBaseFile().get().getFileSize());
  }

  @Test
  void testCreateExternalFileSliceWithWriteStat() {
    HoodieWriteStat writeStat = createMockWriteStat();
    StoragePath basePath = new StoragePath(BASE_PATH);
    FileSlice fileSlice = ExternalFilePathUtil.createExternalFileSlice(basePath, writeStat);
    assertBasicFileSliceProperties(fileSlice, PARTITION_PATH, NORMALIZED_FILE_NAME, COMMIT_TIME);
    assertBaseFileSize(fileSlice, FILE_SIZE);
    assertNoLogFiles(fileSlice);
  }

  @Test
  void testCreateExternalFileSliceWithPartitionAndFileId() {
    StoragePath basePath = new StoragePath(BASE_PATH);
    FileSlice fileSlice = ExternalFilePathUtil.createExternalFileSlice(basePath, PARTITION_PATH, EXTERNAL_FILE_NAME);
    assertBasicFileSliceProperties(fileSlice, PARTITION_PATH, EXTERNAL_FILE_NAME, COMMIT_TIME);
    assertBaseFileSize(fileSlice, 0L);
    assertNoLogFiles(fileSlice);
    String expectedPath = BASE_PATH + "/" + PARTITION_PATH + "/" + NORMALIZED_FILE_NAME;
    assertBaseFilePath(fileSlice, expectedPath);
  }

  @Test
  void testCreateExternalFileSliceWithEmptyPartition() {
    String emptyPartition = "";
    StoragePath basePath = new StoragePath(BASE_PATH);
    FileSlice fileSlice = ExternalFilePathUtil.createExternalFileSlice(basePath, emptyPartition, EXTERNAL_FILE_NAME);
    assertBasicFileSliceProperties(fileSlice, emptyPartition, EXTERNAL_FILE_NAME, COMMIT_TIME);
    String expectedPath = BASE_PATH + "/" + NORMALIZED_FILE_NAME;
    assertBaseFilePath(fileSlice, expectedPath);
  }

  @Test
  void testCreateExternalFileSlicePathConstruction() {
    HoodieWriteStat writeStat = createMockWriteStat();
    StoragePath basePath = new StoragePath(BASE_PATH);
    FileSlice fileSlice = ExternalFilePathUtil.createExternalFileSlice(basePath, writeStat);
    String expectedPath = BASE_PATH + "/" + PARTITION_PATH + "/" + NORMALIZED_FILE_NAME;
    assertBaseFilePath(fileSlice, expectedPath);
  }

  @Test
  void testCreateExternalFileSliceFileGroupId() {
    StoragePath basePath = new StoragePath(BASE_PATH);
    FileSlice fileSlice = ExternalFilePathUtil.createExternalFileSlice(basePath, PARTITION_PATH, EXTERNAL_FILE_NAME);
    assertNotNull(fileSlice.getFileGroupId());
    assertEquals(PARTITION_PATH, fileSlice.getFileGroupId().getPartitionPath());
    assertEquals(EXTERNAL_FILE_NAME, fileSlice.getFileGroupId().getFileId());
  }

  @Test
  void testCreateExternalFileSliceLogFilesEmpty() {
    HoodieWriteStat writeStat = createMockWriteStat();
    StoragePath basePath = new StoragePath(BASE_PATH);
    FileSlice fileSlice1 = ExternalFilePathUtil.createExternalFileSlice(basePath, writeStat);
    FileSlice fileSlice2 = ExternalFilePathUtil.createExternalFileSlice(basePath, PARTITION_PATH, EXTERNAL_FILE_NAME);
    assertNoLogFiles(fileSlice1);
    assertNoLogFiles(fileSlice2);
  }

  @Test
  void testAppendCommitTimeAndExternalFileMarker() {
    // Test the existing method to ensure it works correctly
    String filePath = "/path/to/file.parquet";
    String commitTime = "20240101120000";
    String result = ExternalFilePathUtil.appendCommitTimeAndExternalFileMarker(filePath, commitTime);

    assertEquals("/path/to/file.parquet_20240101120000_hudiext", result);
    assertTrue(ExternalFilePathUtil.isExternallyCreatedFile(result));
  }

  @Test
  void testIsExternallyCreatedFile() {
    String externalFile = "file_20240101120000_hudiext";
    assertTrue(ExternalFilePathUtil.isExternallyCreatedFile(externalFile));
    String regularFile = "file_20240101120000.parquet";
    assertFalse(ExternalFilePathUtil.isExternallyCreatedFile(regularFile));
    assertTrue(ExternalFilePathUtil.isExternallyCreatedFile(EXTERNAL_FILE_NAME));
  }
}
