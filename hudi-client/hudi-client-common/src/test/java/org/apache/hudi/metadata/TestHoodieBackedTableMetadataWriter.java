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

package org.apache.hudi.metadata;

import org.apache.hudi.client.BaseHoodieWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.versioning.v2.ActiveTimelineV2;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;

import org.apache.avro.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.MockedStatic;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TestHoodieBackedTableMetadataWriter {
  private HoodieEngineContext engineContext;
  private HoodieTableMetaClient dataMetaClient;
  private HoodieMetadataConfig metadataConfig;
  private StorageConfiguration<?> storageConf;

  @BeforeEach
  void setUp() {
    engineContext = mock(HoodieEngineContext.class);
    dataMetaClient = mock(HoodieTableMetaClient.class);
    metadataConfig = mock(HoodieMetadataConfig.class);
    storageConf = mock(StorageConfiguration.class);

    when(metadataConfig.getMaxReaderBufferSize()).thenReturn(1024);
  }

  @ParameterizedTest
  @CsvSource(value = {
      "true,true,false,true",
      "false,true,false,true",
      "true,false,false,true",
      "false,false,false,false",
      "false,false,true,false",
  })
  void runPendingTableServicesOperations(boolean hasPendingCompaction, boolean hasPendingLogCompaction, boolean requiresRefresh, boolean ranService) {
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieActiveTimeline initialTimeline = mock(HoodieActiveTimeline.class, RETURNS_DEEP_STUBS);
    BaseHoodieWriteClient writeClient = mock(BaseHoodieWriteClient.class);
    if (requiresRefresh) {
      when(metaClient.reloadActiveTimeline()).thenReturn(initialTimeline);
    } else {
      when(metaClient.getActiveTimeline()).thenReturn(initialTimeline);
    }
    if (hasPendingCompaction) {
      when(initialTimeline.filterPendingCompactionTimeline().countInstants()).thenReturn(1);
    }
    if (hasPendingLogCompaction) {
      when(initialTimeline.filterPendingLogCompactionTimeline().countInstants()).thenReturn(1);
    }
    HoodieActiveTimeline expectedResult;
    if (ranService) {
      HoodieActiveTimeline timelineReloadedAfterServicesRun = mock(HoodieActiveTimeline.class);
      when(metaClient.reloadActiveTimeline()).thenReturn(timelineReloadedAfterServicesRun);
      expectedResult = timelineReloadedAfterServicesRun;
    } else {
      expectedResult = initialTimeline;
    }
    assertSame(expectedResult, HoodieBackedTableMetadataWriter.runPendingTableServicesOperationsAndRefreshTimeline(metaClient, writeClient, requiresRefresh));

    verify(writeClient, times(hasPendingCompaction ? 1 : 0)).runAnyPendingCompactions();
    verify(writeClient, times(hasPendingLogCompaction ? 1 : 0)).runAnyPendingLogCompactions();
    int expectedTimelineReloads = (requiresRefresh ? 1 : 0) + (ranService ? 1 : 0);
    verify(metaClient, times(expectedTimelineReloads)).reloadActiveTimeline();
  }

  @Test
  void rollbackFailedWrites_reloadsTimelineOnWritesRolledBack() {
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder().withPath("file://tmp/")
        .withCleanConfig(HoodieCleanConfig.newBuilder().withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.EAGER).build())
        .build();
    BaseHoodieWriteClient mockWriteClient = mock(BaseHoodieWriteClient.class);
    HoodieTableMetaClient mockMetaClient = mock(HoodieTableMetaClient.class);
    when(mockWriteClient.rollbackFailedWrites(mockMetaClient)).thenReturn(true);
    try (MockedStatic<HoodieTableMetaClient> mockedStatic = mockStatic(HoodieTableMetaClient.class)) {
      HoodieTableMetaClient reloadedClient = mock(HoodieTableMetaClient.class);
      mockedStatic.when(() -> HoodieTableMetaClient.reload(mockMetaClient)).thenReturn(reloadedClient);
      assertSame(reloadedClient, HoodieBackedTableMetadataWriter.rollbackFailedWrites(writeConfig, mockWriteClient, mockMetaClient));
    }
  }

  @Test
  void rollbackFailedWrites_avoidsTimelineReload() {
    HoodieWriteConfig eagerWriteConfig = HoodieWriteConfig.newBuilder().withPath("file://tmp/")
        .withCleanConfig(HoodieCleanConfig.newBuilder().withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.EAGER).build())
        .build();
    BaseHoodieWriteClient mockWriteClient = mock(BaseHoodieWriteClient.class);
    HoodieTableMetaClient mockMetaClient = mock(HoodieTableMetaClient.class);
    when(mockWriteClient.rollbackFailedWrites(mockMetaClient)).thenReturn(false);
    assertSame(mockMetaClient, HoodieBackedTableMetadataWriter.rollbackFailedWrites(eagerWriteConfig, mockWriteClient, mockMetaClient));

    HoodieWriteConfig lazyWriteConfig = HoodieWriteConfig.newBuilder().withPath("file://tmp/")
        .withCleanConfig(HoodieCleanConfig.newBuilder().withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.EAGER).build())
        .build();
    assertSame(mockMetaClient, HoodieBackedTableMetadataWriter.rollbackFailedWrites(lazyWriteConfig, mockWriteClient, mockMetaClient));
  }

  @Test
  void testConvertToColumnStatsRecordWithEmptyInputs() {
    Map<String, Map<String, Long>> partitionFilesToAdd = new HashMap<>();
    Map<String, List<String>> partitionFilesToDelete = new HashMap<>();

    Map<String, HoodieData<HoodieRecord>> result =
        HoodieBackedTableMetadataWriter.convertToColumnStatsRecord(
            partitionFilesToAdd, partitionFilesToDelete, engineContext, dataMetaClient, metadataConfig, Option.empty(), 4);
    assertTrue(result.isEmpty());
  }

  @Test
  void testConvertToColumnStatsRecordWithEmptyColumnsToIndex() {
    // Mock HoodieTableMetadataUtil.getColumnsToIndex to return empty list
    try (MockedStatic<HoodieTableMetadataUtil> mockedUtil = mockStatic(HoodieTableMetadataUtil.class)) {
      Map<String, Object> emptyColumnsMap = new HashMap<>();
      mockedUtil.when(() -> HoodieTableMetadataUtil.getColumnsToIndex(
              any(), any(), any(), eq(false), any()))
          .thenReturn(emptyColumnsMap);

      Map<String, Map<String, Long>> partitionFilesToAdd = new HashMap<>();
      Map<String, Long> filesToAdd = new HashMap<>();
      filesToAdd.put("file1.parquet", 1024L);
      partitionFilesToAdd.put("partition1", filesToAdd);
      Map<String, List<String>> partitionFilesToDelete = new HashMap<>();

      Map<String, HoodieData<HoodieRecord>> result = HoodieBackedTableMetadataWriter.convertToColumnStatsRecord(
          partitionFilesToAdd, partitionFilesToDelete, engineContext, dataMetaClient, metadataConfig,
          Option.empty(), 4);
      assertTrue(result.isEmpty());
    }
  }

  @Test
  void testConvertToColumnStatsRecordWithValidColumns() {
    // Mock HoodieTableMetadataUtil.getColumnsToIndex to return valid columns
    try (MockedStatic<HoodieTableMetadataUtil> mockedUtil = mockStatic(HoodieTableMetadataUtil.class)) {
      Map<String, Object> columnsMap = new HashMap<>();
      columnsMap.put("col1", null);
      columnsMap.put("col2", null);
      mockedUtil.when(() -> HoodieTableMetadataUtil.getColumnsToIndex(
              any(), any(), any(), eq(false), any(), any()))
          .thenReturn(columnsMap);

      // Mock convertFilesToColumnStatsRecords to return empty HoodieData
      HoodieData<HoodieRecord> mockHoodieData = mock(HoodieData.class);
      mockedUtil.when(() -> HoodieTableMetadataUtil.convertFilesToColumnStatsRecords(
              any(), any(), any(), any(), any(), anyInt(), anyInt(), any()))
          .thenReturn(mockHoodieData);

      Map<String, Map<String, Long>> partitionFilesToAdd = new HashMap<>();
      partitionFilesToAdd.put("partition1", new HashMap<>());
      Map<String, List<String>> partitionFilesToDelete = new HashMap<>();

      Map<String, HoodieData<HoodieRecord>> result = HoodieBackedTableMetadataWriter.convertToColumnStatsRecord(
          partitionFilesToAdd, partitionFilesToDelete, engineContext, dataMetaClient, metadataConfig,
          Option.empty(), 4);

      // Verify result contains COLUMN_STATS partition
      assertEquals(1, result.size());
      assertTrue(result.containsKey(MetadataPartitionType.COLUMN_STATS.getPartitionPath()));
      assertEquals(mockHoodieData, result.get(MetadataPartitionType.COLUMN_STATS.getPartitionPath()));

      // Verify the method was called with correct parameters
      mockedUtil.verify(() -> HoodieTableMetadataUtil.convertFilesToColumnStatsRecords(
          eq(engineContext),
          eq(partitionFilesToDelete),
          eq(partitionFilesToAdd),
          eq(dataMetaClient),
          eq(metadataConfig),
          eq(4),
          eq(1024),
          any()
      ));
    }
  }

  @Test
  void testConvertToColumnStatsRecordWithMixedInputs() {
    // Mock HoodieTableMetadataUtil.getColumnsToIndex to return valid columns
    try (MockedStatic<HoodieTableMetadataUtil> mockedUtil = mockStatic(HoodieTableMetadataUtil.class)) {
      Map<String, Object> columnsMap = new HashMap<>();
      columnsMap.put("col1", null);
      columnsMap.put("col2", null);
      columnsMap.put("col3", null);
      mockedUtil.when(() -> HoodieTableMetadataUtil.getColumnsToIndex(
              any(), any(), any(), eq(false), any(), any()))
          .thenReturn(columnsMap);

      // Mock convertFilesToColumnStatsRecords to return empty HoodieData
      HoodieData<HoodieRecord> mockHoodieData = mock(HoodieData.class);
      mockedUtil.when(() -> HoodieTableMetadataUtil.convertFilesToColumnStatsRecords(
              any(), any(), any(), any(), any(), anyInt(), anyInt(), any()))
          .thenReturn(mockHoodieData);

      Map<String, Map<String, Long>> partitionFilesToAdd = new HashMap<>();
      Map<String, Long> filesToAdd = new HashMap<>();
      filesToAdd.put("file1.parquet", 1024L);
      filesToAdd.put("file2.parquet", 2048L);
      partitionFilesToAdd.put("partition1", filesToAdd);

      Map<String, List<String>> partitionFilesToDelete = new HashMap<>();
      List<String> filesToDelete = new ArrayList<>();
      filesToDelete.add("old_file1.parquet");
      filesToDelete.add("old_file2.parquet");
      partitionFilesToDelete.put("partition1", filesToDelete);

      Map<String, HoodieData<HoodieRecord>> result = HoodieBackedTableMetadataWriter.convertToColumnStatsRecord(
          partitionFilesToAdd, partitionFilesToDelete, engineContext, dataMetaClient, metadataConfig,
          Option.empty(), 4);

      // Verify result contains COLUMN_STATS partition.
      assertEquals(1, result.size());
      assertTrue(result.containsKey(MetadataPartitionType.COLUMN_STATS.getPartitionPath()));

      // Verify the method was called with correct parameters.
      mockedUtil.verify(() -> HoodieTableMetadataUtil.convertFilesToColumnStatsRecords(
          eq(engineContext),
          eq(partitionFilesToDelete),
          eq(partitionFilesToAdd),
          eq(dataMetaClient),
          eq(metadataConfig),
          eq(4),
          eq(1024),
          any()
      ));
    }
  }

  @Test
  void testValidateRollbackForMDT() throws Exception {
    List<HoodieInstant> instants = new ArrayList<>();

    instants.add(INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.DELTA_COMMIT_ACTION, "20250925012123905"));

    instants.add(INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "20250925012447357", "20250925013432341"));
    instants.add(INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "20250925012518125", "20250925012831379"));
    instants.add(INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "20250925012851950", "20250925013157886"));

    instants.add(INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "20250925012523368", "20250925015000000"));

    // a deltacommit instant requested before the compaction request time and finished after it.
    HoodieInstant instantToRollback = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "20250925012123804", "20250925014634434");
    instants.add(instantToRollback);

    HoodieActiveTimeline timeline = createMockTimeline(instants);

    HoodieBackedTableMetadataWriter<?, ?> writer = mock(HoodieBackedTableMetadataWriter.class);

    HoodieTableMetaClient mockMetaClient = mock(HoodieTableMetaClient.class);
    when(mockMetaClient.getActiveTimeline()).thenReturn(timeline);

    java.lang.reflect.Field metadataMetaClientField = HoodieBackedTableMetadataWriter.class.getDeclaredField("metadataMetaClient");
    metadataMetaClientField.setAccessible(true);
    metadataMetaClientField.set(writer, mockMetaClient);

    java.lang.reflect.Method validateRollbackMethod = HoodieBackedTableMetadataWriter.class.getDeclaredMethod("validateRollback", HoodieInstant.class);
    validateRollbackMethod.setAccessible(true);

    assertDoesNotThrow(() -> validateRollbackMethod.invoke(writer, instantToRollback));
  }

  @SuppressWarnings("deprecation")
  private HoodieActiveTimeline createMockTimeline(List<HoodieInstant> instants) {
    ActiveTimelineV2 timeline = new ActiveTimelineV2();
    timeline.setInstants(instants);
    return timeline;
  }

  @ParameterizedTest
  @CsvSource(value = {
      "true,0",   // clean instant already exists, clean should not be called
      "false,1"   // clean instant does not exist, clean should be called once
  })
  void cleanIfNecessary_skipsDuplicateCleanInstants(boolean cleanInstantExists, int expectedCleanCalls,
                                                    @TempDir java.nio.file.Path tempDir) throws Exception {
    String basePath = tempDir.toString();
    String instantTime = "20230101120000";
    String cleanInstant = "20230101120000999"; // HoodieTableMetadataUtil.createCleanTimestamp format

    // Create a real Hudi table
    StorageConfiguration storageConfiguration = new HadoopStorageConfiguration(false);
    HoodieTableMetaClient.newTableBuilder()
        .setTableType(org.apache.hudi.common.model.HoodieTableType.COPY_ON_WRITE)
        .setTableName("test_table")
        .setPayloadClassName(org.apache.hudi.common.model.HoodieAvroPayload.class.getName())
        .initTable(storageConfiguration, basePath);

    HoodieTableMetaClient metadataMetaClient = mock(HoodieTableMetaClient.class, RETURNS_DEEP_STUBS);
    HoodieActiveTimeline activeTimeline = mock(HoodieActiveTimeline.class, RETURNS_DEEP_STUBS);
    BaseHoodieWriteClient writeClient = mock(BaseHoodieWriteClient.class);

    // Mock the timeline structure for metadata metaClient
    when(metadataMetaClient.getActiveTimeline()).thenReturn(activeTimeline);
    when(metadataMetaClient.getCommitTimeline().filterCompletedInstants().lastInstant())
        .thenReturn(Option.empty()); // No recent compaction to skip clean

    // Mock the cleaner timeline check
    when(activeTimeline.getCleanerTimeline().filterCompletedInstants().containsInstant(cleanInstant))
        .thenReturn(cleanInstantExists);

    try (MockedStatic<HoodieBackedTableMetadataWriterTableVersionSix> mockedUtil = mockStatic(HoodieBackedTableMetadataWriterTableVersionSix.class)) {
      mockedUtil.when(() -> HoodieBackedTableMetadataWriterTableVersionSix.createCleanTimestamp(instantTime))
          .thenReturn(cleanInstant);

      // Create a concrete test implementation
      TestableHoodieBackedTableMetadataWriter writer = new TestableHoodieBackedTableMetadataWriter(basePath, metadataMetaClient);

      // Execute
      writer.cleanIfNecessary(writeClient, instantTime);

      // Verify
      verify(writeClient, times(expectedCleanCalls)).clean(cleanInstant);
      verify(writeClient, times(1)).lazyRollbackFailedIndexing();
    }
  }

  /**
   * Test implementation of HoodieBackedTableMetadataWriter to expose protected methods for testing.
   */
  private static class TestableHoodieBackedTableMetadataWriter extends HoodieBackedTableMetadataWriterTableVersionSix<Object, Object> {
    private final HoodieTableMetaClient metadataMetaClient;

    TestableHoodieBackedTableMetadataWriter(String basePath, HoodieTableMetaClient metadataMetaClient) {
      super(
          new HadoopStorageConfiguration(false),
          HoodieWriteConfig.newBuilder().withPath(basePath).build(),
          HoodieFailedWritesCleaningPolicy.EAGER,
          null, // engineContext not needed for this test
          Option.empty()
      );
      this.metadataMetaClient = metadataMetaClient;
    }

    @Override
    protected boolean initializeIfNeeded(HoodieTableMetaClient dataMetaClient,
                                         Option<String> inflightInstantTimestamp) throws IOException {
      // Return false to avoid initialization and keep this.metadata null
      return false;
    }

    @Override
    protected EngineType getEngineType() {
      return null;
    }

    @Override
    protected HoodieData<HoodieRecord> getExpressionIndexRecords(List partitionFilePathAndSizeTriplet, HoodieIndexDefinition indexDefinition, HoodieTableMetaClient metaClient, int parallelism,
                                                                 Schema tableSchema, Schema readerSchema, StorageConfiguration storageConf, String instantTime) {
      return null;
    }

    @Override
    protected void updateColumnsToIndexWithColStats(List columnsToIndex) {
      // No-op for testing
    }

    @Override
    protected HoodieTableMetaClient getMetadataMetaClient() {
      return metadataMetaClient;
    }

    @Override
    protected void initRegistry() {
      // No-op for testing
    }

    @Override
    public void close() throws Exception {
      // No-op for testing
    }

    @Override
    protected void commit(String instantTime, Map<String, HoodieData<HoodieRecord>> partitionRecordsMap) {
      // No-op for testing
    }

    @Override
    protected Object convertHoodieDataToEngineSpecificData(HoodieData<HoodieRecord> records) {
      return null;
    }

    @Override
    protected HoodieData<WriteStatus> convertEngineSpecificDataToHoodieData(Object records) {
      return null;
    }

    @Override
    protected void bulkCommit(String instantTime, String relativePartitionPath, HoodieData records, MetadataTableFileGroupIndexParser indexParser) {
      // No-op for testing
    }

    @Override
    protected void upsertAndCommit(BaseHoodieWriteClient writeClient, String instantTime, Object preppedRecordInputs, List fileGroupsIdsToUpdate) {
      // No-op for testing
    }

    @Override
    protected void upsertAndCommit(BaseHoodieWriteClient writeClient, String instantTime, Object preppedRecordInputs) {
      // No-op for testing
    }

    @Override
    protected void bulkInsertAndCommit(BaseHoodieWriteClient writeClient, String instantTime, Object preppedRecordInputs, Option bulkInsertPartitioner) {
      // No-op for testing
    }

    @Override
    protected BaseHoodieWriteClient<?, Object, ?, Object> initializeWriteClient() {
      return null;
    }

    @Override
    public void deletePartitions(String instantTime, List<MetadataPartitionType> partitions) {
      // No-op for testing
    }
  }
}
