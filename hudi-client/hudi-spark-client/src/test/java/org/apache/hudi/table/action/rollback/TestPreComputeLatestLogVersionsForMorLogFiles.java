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

package org.apache.hudi.table.action.rollback;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.apache.hudi.avro.model.HoodieRollbackRequest;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathFilter;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.table.HoodieTable;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * Unit tests for BaseRollbackHelper#preComputeLatestLogVersionsForMorLogFiles(...)
 *
 */
class TestPreComputeLatestLogVersionsForMorLogFiles {

  @Mock
  private HoodieTable table;
  @Mock
  private HoodieTableMetaClient metaClient;
  @Mock
  private HoodieWriteConfig config;
  @Mock
  private HoodieStorage storage;

  private RollbackHelperV1 helper;

  private static final StoragePath BASE_PATH = new StoragePath("file:/tmp/hudi-base");

  @BeforeEach
  void setUp() {
    MockitoAnnotations.openMocks(this);

    when(table.getMetaClient()).thenReturn(metaClient);
    when(metaClient.getBasePath()).thenReturn(BASE_PATH); // your fork uses Path
    when(metaClient.getStorage()).thenReturn(storage);

    helper = new RollbackHelperV1(table, config);
  }

  @Test
  void returnsEmpty_whenNoRequestsHaveLogBlocks() {
    // logBlocksToBeDeleted is EMPTY => request is filtered out in method
    SerializableHoodieRollbackRequest r1 =
        serializableReq("p1", "file-1", "000111", Collections.emptyMap());
    SerializableHoodieRollbackRequest r2 =
        serializableReq("p2", "file-2", "000222", Collections.emptyMap());

    Map<String, Map<Pair<String, String>, Pair<Integer, String>>> out =
        helper.preComputeLatestLogVersionsForMorLogFiles(Arrays.asList(r1, r2));

    assertTrue(out.isEmpty());
    verifyNoInteractions(storage);
  }

  @Test
  void preCompute_singlePartition_picksLatestVersionAndToken() throws Exception {
    String partition = "p1";
    String fileId = "file-1";
    String baseInstant = "000111";

    SerializableHoodieRollbackRequest req =
        serializableReq(partition, fileId, baseInstant, Collections.singletonMap("blk1", 1L));

    // Two log files for the same (fileId, baseInstant). Expect "latest" to win.
    String logV1 = logName(fileId, baseInstant, 1, "1-0-1");
    String logV2 = logName(fileId, baseInstant, 2, "1-0-2");

    when(storage.listDirectEntries(eq(new StoragePath(BASE_PATH, partition)), any(StoragePathFilter.class)))
        .thenReturn(Arrays.asList(
            generateStoragePathInfo(partition, logV1),
            generateStoragePathInfo(partition, logV2)));

    Map<String, Map<Pair<String, String>, Pair<Integer, String>>> out =
        helper.preComputeLatestLogVersionsForMorLogFiles(Arrays.asList(req));
    assertTrue(out.containsKey(partition));

    validateLatestLogFileVersionAndToken(out.get(partition), Pair.of(fileId, baseInstant), 2, "1-0-2");
    verify(storage, times(1)).listDirectEntries(eq(new StoragePath(BASE_PATH, partition)), any(StoragePathFilter.class));
    verifyNoMoreInteractions(storage);
  }

  @Test
  void twoPartitions_listStatusCalledOncePerPartition() throws Exception {
    SerializableHoodieRollbackRequest r1 =
        serializableReq("p1", "file-1", "000111", Collections.singletonMap("blk1", 1L));
    SerializableHoodieRollbackRequest r2 =
        serializableReq("p2", "file-2", "000222", Collections.singletonMap("blk1", 1L));

    when(storage.listDirectEntries(eq(new StoragePath(BASE_PATH, "p1")), any(StoragePathFilter.class)))
        .thenReturn(Arrays.asList(generateStoragePathInfo("p1", logName("file-1", "000111", 1, "1-0-1"))));

    when(storage.listDirectEntries(eq(new StoragePath(BASE_PATH, "p2")), any(StoragePathFilter.class)))
        .thenReturn(Arrays.asList(
            generateStoragePathInfo("p2", logName("file-2", "000222", 2, "1-0-2"))));

    Map<String, Map<Pair<String, String>, Pair<Integer, String>>> out =
        helper.preComputeLatestLogVersionsForMorLogFiles(Arrays.asList(r1, r2));

    assertTrue(out.containsKey("p1"));
    assertTrue(out.containsKey("p2"));

    validateLatestLogFileVersionAndToken(out.get("p1"), Pair.of("file-1", "000111"), 1, "1-0-1");
    validateLatestLogFileVersionAndToken(out.get("p2"), Pair.of("file-2", "000222"), 2, "1-0-2");

    verify(storage, times(1)).listDirectEntries(eq(new StoragePath(BASE_PATH, "p1")), any(StoragePathFilter.class));
    verify(storage, times(1)).listDirectEntries(eq(new StoragePath(BASE_PATH, "p2")), any(StoragePathFilter.class));
    verifyNoMoreInteractions(storage);
  }

  private void validateLatestLogFileVersionAndToken(Map<Pair<String, String>, Pair<Integer, String>> perPartition, Pair<String, String> fileIdBaseInstant,
                                                    int expectedVersion, String expectedWriteToken) {
    assertTrue(perPartition.containsKey(fileIdBaseInstant));
    Pair<Integer, String> versionAndToken = perPartition.get(fileIdBaseInstant);
    assertEquals(expectedVersion, versionAndToken.getLeft());
    assertEquals(expectedWriteToken, versionAndToken.getRight());
  }

  @Test
  void ignoresOtherFileIdOrBaseInstant_notInRollbackRequests() throws Exception {
    String partition = "p1";
    String requestedFileId = "file-1";
    String requestedBaseInstant = "000111";

    SerializableHoodieRollbackRequest req =
        serializableReq(partition, requestedFileId, requestedBaseInstant, Collections.singletonMap("blk1", 1L));

    // listStatus returns:
    // - one matching log for (file-1, 000111)
    // - one other fileId for same base instant
    // - one same fileId but different base instant
    String matching = logName(requestedFileId, requestedBaseInstant, 2, "1-0-2");
    String otherFileId = logName("file-OTHER", requestedBaseInstant, 99, "9-9-9");
    String otherBaseInstant = logName(requestedFileId, "000999", 100, "9-9-8");

    when(storage.listDirectEntries(eq(new StoragePath(BASE_PATH, partition)), any(StoragePathFilter.class)))
        .thenReturn(Arrays.asList(
            generateStoragePathInfo(partition, matching),
            generateStoragePathInfo(partition, otherFileId),
            generateStoragePathInfo(partition, otherBaseInstant)));

    Map<String, Map<Pair<String, String>, Pair<Integer, String>>> out =
        helper.preComputeLatestLogVersionsForMorLogFiles(Collections.singletonList(req));

    assertTrue(out.containsKey(partition));

    Map<Pair<String, String>, Pair<Integer, String>> perPartition = out.get(partition);
    // Only the requested (fileId, baseInstant) should be present
    assertEquals(1, perPartition.size());
    validateLatestLogFileVersionAndToken(out.get(partition), Pair.of(requestedFileId, requestedBaseInstant), 2, "1-0-2");
  }

  // ---------------- helpers ----------------

  /**
   * Build a real SerializableHoodieRollbackRequest by mocking HoodieRollbackRequest.
   * This is the easiest way to control logBlocksToBeDeleted (since Serializable... has no setters).
   */
  private static SerializableHoodieRollbackRequest serializableReq(
      String partition, String fileId, String latestBaseInstant, Map<String, Long> logBlocks) {

    HoodieRollbackRequest rr = mock(HoodieRollbackRequest.class);
    when(rr.getPartitionPath()).thenReturn(partition);
    when(rr.getFileId()).thenReturn(fileId);
    when(rr.getLatestBaseInstant()).thenReturn(latestBaseInstant);
    when(rr.getFilesToBeDeleted()).thenReturn(Collections.emptyList());
    when(rr.getLogBlocksToBeDeleted()).thenReturn(logBlocks);

    return new SerializableHoodieRollbackRequest(rr);
  }

  private static String logName(String fileId, String baseInstant, int version, String writeToken) {
    return FSUtils.makeLogFileName(fileId, HoodieLogFile.DELTA_EXTENSION, baseInstant, version, writeToken);
  }

  private static StoragePathInfo generateStoragePathInfo(String partition, String fileName) {
    return new StoragePathInfo(new StoragePath(new StoragePath(BASE_PATH, partition), fileName),
        100L, false, (short)1, 128 * 1024 * 1024L, 0L);
  }
}
