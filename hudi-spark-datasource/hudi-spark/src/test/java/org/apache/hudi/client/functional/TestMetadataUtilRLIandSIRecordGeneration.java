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

package org.apache.hudi.client.functional;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.model.HoodieMetadataRecord;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteClientTestUtils;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.EmptyHoodieRecordPayload;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.read.DeleteContext;
import org.apache.hudi.common.table.read.HoodieFileGroupReader;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.ExternalFilePathUtil;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.metadata.BaseFileRecordParsingUtils;
import org.apache.hudi.metadata.HoodieIndexVersion;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.testutils.HoodieClientTestBase;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.DELTA_COMMIT_ACTION;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.AVRO_SCHEMA;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.convertMetadataToRecordIndexRecords;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.getRevivedAndDeletedKeysFromMergedLogs;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.reduceByKeys;
import static org.apache.hudi.metadata.SecondaryIndexKeyUtils.constructSecondaryIndexKey;
import static org.apache.hudi.metadata.SecondaryIndexKeyUtils.getRecordKeyFromSecondaryIndexKey;
import static org.apache.hudi.metadata.SecondaryIndexRecordGenerationUtils.convertWriteStatsForNonNativeFormatToSecondaryIndexRecords;
import static org.apache.hudi.metadata.SecondaryIndexRecordGenerationUtils.convertWriteStatsToSecondaryIndexRecords;
import static org.apache.hudi.metadata.SecondaryIndexRecordGenerationUtils.readSecondaryKeysFromFileSlices;
import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class TestMetadataUtilRLIandSIRecordGeneration extends HoodieClientTestBase {

  /**
   * Tests various methods used for RLI and SI record generation flows.
   * We test below methods
   * BaseFileRecordParsingUtils.generateRLIMetadataHoodieRecordsForBaseFile(...). This is used for RLI record generation.
   * BaseFileRecordParsingUtils.getRecordKeyStatuses(...) // This is used in both RLI and SI flow.
   *
   * @throws IOException
   */
  @Test
  public void testRecordGenerationAPIsForCOW() throws IOException {
    HoodieTableType tableType = HoodieTableType.COPY_ON_WRITE;
    cleanupClients();
    initMetaClient(tableType);
    cleanupTimelineService();
    initTimelineService();

    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);
    HoodieWriteConfig writeConfig = getConfigBuilder(HoodieFailedWritesCleaningPolicy.EAGER).build();

    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, writeConfig)) {
      // Insert
      String commitTime = client.startCommit();
      List<HoodieRecord> records1 = dataGen.generateInserts(commitTime, 100);
      List<WriteStatus> writeStatusList1 = client.insert(jsc.parallelize(records1, 1), commitTime).collect();
      client.commit(commitTime, jsc.parallelize(writeStatusList1));
      assertNoWriteErrors(writeStatusList1);

      // assert RLI records for a base file from 1st commit
      String finalCommitTime = commitTime;
      Map<String, String> recordKeyToPartitionMapping1 = new HashMap<>();
      Map<String, String> fileIdToFileNameMapping1 = new HashMap<>();
      writeStatusList1.forEach(writeStatus -> {
        assertEquals(writeStatus.getStat().getNumDeletes(), 0);
        // Fetch record keys for all
        String writeStatFileId = writeStatus.getFileId();
        if (!fileIdToFileNameMapping1.containsKey(writeStatFileId)) {
          fileIdToFileNameMapping1.put(writeStatFileId, writeStatus.getStat().getPath().substring(writeStatus.getStat().getPath().lastIndexOf("/") + 1));
        }

        // poll into generateRLIMetadataHoodieRecordsForBaseFile to fetch MDT RLI records for inserts and deletes.
        Iterator<HoodieRecord> rliRecordsItr = BaseFileRecordParsingUtils.generateRLIMetadataHoodieRecordsForBaseFile(metaClient.getBasePath().toString(),
            writeStatus.getStat(), writeConfig.getWritesFileIdEncoding(), finalCommitTime, metaClient.getStorage(), false);
        while (rliRecordsItr.hasNext()) {
          HoodieRecord rliRecord = rliRecordsItr.next();
          String key = rliRecord.getRecordKey();
          String partition = ((HoodieMetadataPayload) rliRecord.getData()).getDataPartition();
          recordKeyToPartitionMapping1.put(key, partition);
        }
      });

      Map<String, String> expectedRecordToPartitionMapping1 = new HashMap<>();
      records1.forEach(record -> expectedRecordToPartitionMapping1.put(record.getRecordKey(), record.getPartitionPath()));

      assertEquals(expectedRecordToPartitionMapping1, recordKeyToPartitionMapping1);

      // lets update some records and assert RLI records.
      commitTime = client.startCommit();
      String finalCommitTime2 = commitTime;
      List<HoodieRecord> deletes2 = dataGen.generateUniqueDeleteRecords(commitTime, 30);
      List<HoodieRecord> updates2 = dataGen.generateUniqueUpdates(commitTime, 30);
      List<HoodieRecord> inserts2 = dataGen.generateInserts(commitTime, 30);
      List<HoodieRecord> records2 = new ArrayList<>();
      records2.addAll(inserts2);
      records2.addAll(updates2);
      records2.addAll(deletes2);

      JavaRDD<WriteStatus> rawWriteStatuses2 = client.upsert(jsc.parallelize(records2, 1), commitTime);
      JavaRDD<WriteStatus> writeStatusesRDD2 = jsc.parallelize(rawWriteStatuses2.collect(), 1);
      List<WriteStatus> writeStatuses2 = writeStatusesRDD2.collect();

      assertNoWriteErrors(writeStatuses2);

      List<String> expectedInserts = inserts2.stream().map(record -> record.getKey().getRecordKey()).collect(Collectors.toList());
      List<String> expectedDeletes = deletes2.stream().map(record -> record.getKey().getRecordKey()).collect(Collectors.toList());
      List<String> actualInserts = new ArrayList<>();
      List<String> actualDeletes = new ArrayList<>();
      // only inserts and deletes will result in RLI records. lets validate that.
      generateRliRecordsAndAssert(writeStatuses2, fileIdToFileNameMapping1, finalCommitTime2, writeConfig, actualInserts, actualDeletes);

      assertListEquality(expectedInserts, actualInserts);
      assertListEquality(expectedDeletes, actualDeletes);

      // lets validate APIs in BaseFileParsingUtils directly
      actualInserts = new ArrayList<>();
      actualDeletes = new ArrayList<>();
      List<String> actualUpdates = new ArrayList<>();
      List<String> expectedUpdates = updates2.stream().map(record -> record.getKey().getRecordKey()).collect(Collectors.toList());
      parseRecordKeysFromBaseFiles(writeStatuses2, fileIdToFileNameMapping1, finalCommitTime2, writeConfig, actualInserts, actualDeletes, actualUpdates);
      assertListEquality(expectedInserts, actualInserts);
      assertListEquality(expectedDeletes, actualDeletes);
      // we can't really assert equality for updates. bcoz, w/ COW, we might just rewrite an existing parquet file. So, more records will be deduced as updates.
      // And so, we are validating using contains.
      expectedUpdates.forEach(entry -> assertTrue(actualUpdates.contains(entry)));
    }
  }

  /**
   * Tests various methods used for RLI and SI record generation flows w/ MOR table. here emphasis are given to log files.
   * We test below methods
   * BaseFileRecordParsingUtils.generateRLIMetadataHoodieRecordsForBaseFile(...). This is used for RLI record generation.
   * HoodieTableMetadataUtil.getRecordKeys() // This is used in both RLI and SI flow.
   * HoodieTableMetadataUtil.getRecordKeysDeletedOrUpdated() for HoodieCommitMetadata.
   * <p>
   * We also test few adhoc scenarios.
   * - if any log files contains inserts, RLI and SI record generation should throw exception.
   * - RLI do no generate any records for compaction operation.
   *
   * @throws IOException
   */
  @Test
  public void testRecordGenerationAPIsForMOR() throws IOException {
    HoodieTableType tableType = HoodieTableType.MERGE_ON_READ;
    cleanupClients();
    Properties props = new Properties();
    props.put(HoodieTableConfig.ORDERING_FIELDS.key(), "timestamp");
    initMetaClient(tableType, props);
    cleanupTimelineService();
    initTimelineService();

    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);
    HoodieWriteConfig writeConfig = getConfigBuilder(HoodieFailedWritesCleaningPolicy.EAGER)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().withMaxNumDeltaCommitsBeforeCompaction(3)
            .withInlineCompaction(false)
            .compactionSmallFileSize(0).build()).build();

    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, writeConfig)) {
      // Insert
      String commitTime = client.startCommit();
      List<HoodieRecord> records1 = dataGen.generateInserts(commitTime, 100);
      List<WriteStatus> writeStatusList = client.insert(jsc.parallelize(records1, 1), commitTime).collect();
      client.commit(commitTime, jsc.parallelize(writeStatusList), Option.empty(), DELTA_COMMIT_ACTION, Collections.emptyMap(), Option.empty());
      assertNoWriteErrors(writeStatusList);

      // assert RLI records for a base file from 1st commit
      String finalCommitTime = commitTime;
      Map<String, String> recordKeyToPartitionMapping1 = new HashMap<>();
      Map<String, String> fileIdToFileNameMapping1 = new HashMap<>();
      writeStatusList.forEach(writeStatus -> {
        assertEquals(writeStatus.getStat().getNumDeletes(), 0);
        // Fetch record keys for all
        String writeStatFileId = writeStatus.getFileId();
        if (!fileIdToFileNameMapping1.containsKey(writeStatFileId)) {
          fileIdToFileNameMapping1.put(writeStatFileId, writeStatus.getStat().getPath().substring(writeStatus.getStat().getPath().lastIndexOf("/") + 1));
        }

        // poll into generateRLIMetadataHoodieRecordsForBaseFile to fetch MDT RLI records for inserts and deletes.
        Iterator<HoodieRecord> rliRecordsItr = BaseFileRecordParsingUtils.generateRLIMetadataHoodieRecordsForBaseFile(metaClient.getBasePath().toString(),
            writeStatus.getStat(), writeConfig.getWritesFileIdEncoding(), finalCommitTime, metaClient.getStorage(), writeConfig.isRecordLevelIndexEnabled());
        while (rliRecordsItr.hasNext()) {
          HoodieRecord rliRecord = rliRecordsItr.next();
          String key = rliRecord.getRecordKey();
          String partition = ((HoodieMetadataPayload) rliRecord.getData()).getDataPartition();
          recordKeyToPartitionMapping1.put(key, partition);
        }
      });

      Map<String, String> expectedRecordToPartitionMapping1 = new HashMap<>();
      records1.forEach(record -> expectedRecordToPartitionMapping1.put(record.getRecordKey(), record.getPartitionPath()));

      assertEquals(expectedRecordToPartitionMapping1, recordKeyToPartitionMapping1);

      // lets update some records and assert RLI records.
      commitTime = client.startCommit();
      List<HoodieRecord> deletes2 = dataGen.generateUniqueDeleteRecords(commitTime, 30);
      List<HoodieRecord> updates2 = dataGen.generateUniqueUpdates(commitTime, 30);
      List<HoodieRecord> inserts2 = dataGen.generateInserts(commitTime, 30);
      List<HoodieRecord> records2 = new ArrayList<>();
      records2.addAll(inserts2);
      records2.addAll(updates2);
      records2.addAll(deletes2);

      List<WriteStatus> writeStatusList2 = client.upsert(jsc.parallelize(records2, 1), commitTime).collect();
      client.commit(commitTime, jsc.parallelize(writeStatusList2), Option.empty(), DELTA_COMMIT_ACTION, Collections.emptyMap(), Option.empty());

      assertRLIandSIRecordGenerationAPIs(inserts2, updates2, deletes2, writeStatusList2, commitTime, writeConfig);

      // trigger 2nd commit.
      commitTime = client.startCommit();
      String finalCommitTime3 = commitTime;
      List<HoodieRecord> deletes3 = dataGen.generateUniqueDeleteRecords(commitTime, 30);
      List<HoodieRecord> updates3 = dataGen.generateUniqueUpdates(commitTime, 30);
      List<HoodieRecord> inserts3 = dataGen.generateInserts(commitTime, 30);
      List<HoodieRecord> records3 = new ArrayList<>();
      records3.addAll(inserts3);
      records3.addAll(updates3);
      records3.addAll(deletes3);

      List<WriteStatus> writeStatusList3 = client.upsert(jsc.parallelize(records3, 1), commitTime).collect();
      client.commit(commitTime, jsc.parallelize(writeStatusList3), Option.empty(), DELTA_COMMIT_ACTION, Collections.emptyMap(), Option.empty());
      assertRLIandSIRecordGenerationAPIs(inserts3, updates3, deletes3, writeStatusList3, finalCommitTime3, writeConfig);

      // trigger compaction
      Option<String> compactionInstantOpt = client.scheduleCompaction(Option.empty());
      assertTrue(compactionInstantOpt.isPresent());
      HoodieWriteMetadata compactionWriteMetadata = client.compact(compactionInstantOpt.get());
      client.commitCompaction(compactionInstantOpt.get(), compactionWriteMetadata, Option.empty());
      assertTrue(metaClient.reloadActiveTimeline().filterCompletedInstants().containsInstant(compactionInstantOpt.get()));
      HoodieCommitMetadata compactionCommitMetadata = (HoodieCommitMetadata) compactionWriteMetadata.getCommitMetadata().get();
      // no RLI records should be generated for compaction operation.
      assertTrue(convertMetadataToRecordIndexRecords(context, compactionCommitMetadata, writeConfig.getMetadataConfig(),
          metaClient, writeConfig.getWritesFileIdEncoding(), compactionInstantOpt.get(), EngineType.SPARK, writeConfig.enableOptimizedLogBlocksScan()).isEmpty());
    }
  }

  /**
   * Tests methods used for Secondary Index record generation flows with MOR table during index initialization as well as update.
   */
  @Test
  public void testSecondaryIndexRecordGenerationForMOR() throws IOException {
    HoodieTableType tableType = HoodieTableType.MERGE_ON_READ;
    cleanupClients();
    Properties props = new Properties();
    props.put(HoodieTableConfig.ORDERING_FIELDS.key(), "timestamp");
    initMetaClient(tableType, props);
    cleanupTimelineService();
    initTimelineService();

    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);
    HoodieWriteConfig writeConfig = getConfigBuilder(HoodieFailedWritesCleaningPolicy.EAGER)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().withMaxNumDeltaCommitsBeforeCompaction(3)
            .withInlineCompaction(false)
            .compactionSmallFileSize(0).build()).build();

    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, writeConfig)) {
      // Insert
      String commitTime = client.startCommit();
      int initialRecordsCount = 10;
      List<HoodieRecord> records1 = dataGen.generateInserts(commitTime, initialRecordsCount);
      List<WriteStatus> writeStatusList = client.insert(jsc.parallelize(records1, 1), commitTime).collect();
      assertNoWriteErrors(writeStatusList);
      client.commit(commitTime, jsc.parallelize(writeStatusList));

      // assert SI records from 1st commit
      List<String> expectedSecondaryIndexKeys = records1.stream().map(TestMetadataUtilRLIandSIRecordGeneration::getSecondaryIndexKey).collect(Collectors.toList());
      String firstCommitTime = commitTime;
      HoodieIndexDefinition indexDefinition = HoodieIndexDefinition.newBuilder()
          .withIndexName("secondary_index_idx_rider")
          .withIndexType(MetadataPartitionType.COLUMN_STATS.name())
          .withVersion(HoodieIndexVersion.getCurrentVersion(HoodieTableVersion.current(), "secondary_index_idx_rider"))
          .withIndexFunction("")
          .withSourceFields(Collections.singletonList("rider"))
          .withIndexOptions(Collections.emptyMap())
          .build();
      HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder().enable(true).withSecondaryIndexParallelism(2).build();
      HoodieTableMetadata metadata = metaClient.getTableFormat().getMetadataFactory().create(engineContext, storage, metadataConfig, metaClient.getBasePath().toString());
      HoodieTableFileSystemView metadataView = new HoodieTableFileSystemView(metadata, metaClient, metaClient.getActiveTimeline());
      metadataView.loadAllPartitions();
      List<Pair<String, FileSlice>> partitionFileSlicePairs = new ArrayList<>();
      HoodieTableFileSystemView finalMetadataView = metadataView;
      Arrays.asList(dataGen.getPartitionPaths()).forEach(partition -> finalMetadataView.getLatestMergedFileSlicesBeforeOrOn(partition, firstCommitTime)
          .forEach(fs -> partitionFileSlicePairs.add(Pair.of(partition, fs))));
      List<HoodieRecord> secondaryIndexRecords = readSecondaryKeysFromFileSlices(
          engineContext, partitionFileSlicePairs, metadataConfig.getSecondaryIndexParallelism(), this.getClass().getSimpleName(), metaClient, indexDefinition, writeConfig.getProps()).collectAsList();
      assertListEquality(expectedSecondaryIndexKeys, secondaryIndexRecords.stream().map(HoodieRecord::getRecordKey).collect(Collectors.toList()));

      // update and insert some records and assert SI records
      commitTime = client.startCommit();
      List<HoodieRecord> updates2 = dataGen.generateUniqueUpdates(commitTime, 1);
      List<String> expectedUpdatedIndexKeys = updates2.stream().map(TestMetadataUtilRLIandSIRecordGeneration::getSecondaryIndexKey).collect(Collectors.toList());
      List<HoodieRecord> inserts2 = dataGen.generateInserts(commitTime, 1);
      List<String> expectedInsertedIndexKeys = inserts2.stream().map(TestMetadataUtilRLIandSIRecordGeneration::getSecondaryIndexKey).collect(Collectors.toList());
      List<HoodieRecord> records2 = new ArrayList<>();
      records2.addAll(inserts2);
      records2.addAll(updates2);
      List<WriteStatus> writeStatusList2 = client.upsert(jsc.parallelize(records2, 1), commitTime).collect();
      assertNoWriteErrors(writeStatusList2);

      // assert SI
      String secondCommitTime = commitTime;
      metaClient = HoodieTableMetaClient.reload(metaClient);
      metadata.reset();
      metadataView = new HoodieTableFileSystemView(metadata, metaClient, metaClient.getActiveTimeline());
      List<HoodieWriteStat> allWriteStats = writeStatusList2.stream().map(WriteStatus::getStat).collect(Collectors.toList());
      Schema tableSchema = HoodieAvroUtils.addMetadataFields(new Schema.Parser().parse(TRIP_EXAMPLE_SCHEMA));
      secondaryIndexRecords =
          convertWriteStatsToSecondaryIndexRecords(allWriteStats, secondCommitTime, tableSchema, indexDefinition, metadataConfig, metaClient, engineContext, writeConfig).collectAsList();
      client.commit(secondCommitTime, jsc.parallelize(writeStatusList2));

      // There should be 3 SI records:
      // a) 1 insert due to inserts2,
      // b) 1 delete + 1 insert due to updates2,
      assertEquals(3, secondaryIndexRecords.size());
      List<String> validSecondaryIndexKeys = new ArrayList<>(expectedInsertedIndexKeys);
      validSecondaryIndexKeys.addAll(expectedUpdatedIndexKeys);
      // filter delete records from secondaryIndexRecords
      List<HoodieRecord> deletedSecondaryIndexRecords = new ArrayList<>();
      List<HoodieRecord> validSecondaryIndexRecords = new ArrayList<>();
      secondaryIndexRecords.forEach(record -> {
        populateValidAndDeletedSecondaryIndexRecords(record, deletedSecondaryIndexRecords, validSecondaryIndexRecords);
      });
      assertListEquality(validSecondaryIndexKeys, validSecondaryIndexRecords.stream().map(HoodieRecord::getRecordKey).collect(Collectors.toList()));
      assertTrue(expectedSecondaryIndexKeys.containsAll(deletedSecondaryIndexRecords.stream().map(HoodieRecord::getRecordKey).collect(Collectors.toList())));

      // let us create one more file slice with delete
      commitTime = client.startCommit();
      List<HoodieRecord> deletes = dataGen.generateUniqueDeleteRecords(commitTime, 1);
      List<String> expectedDeletedIndexKeys = deletes.stream().map(TestMetadataUtilRLIandSIRecordGeneration::getSecondaryIndexKey).collect(Collectors.toList());
      List<HoodieRecord> records3 = new ArrayList<>();
      records3.addAll(deletes);
      List<WriteStatus> writeStatusList3 = client.upsert(jsc.parallelize(records3, 1), commitTime).collect();
      assertNoWriteErrors(writeStatusList3);

      // assert SI
      String thirdCommitTime = commitTime;
      metaClient = HoodieTableMetaClient.reload(metaClient);
      metadata.reset();
      metadataView = new HoodieTableFileSystemView(metadata, metaClient, metaClient.getActiveTimeline());
      allWriteStats = writeStatusList3.stream().map(WriteStatus::getStat).collect(Collectors.toList());
      secondaryIndexRecords =
          convertWriteStatsToSecondaryIndexRecords(allWriteStats, thirdCommitTime, tableSchema, indexDefinition, metadataConfig, metaClient, engineContext, writeConfig).collectAsList();
      client.commit(thirdCommitTime, jsc.parallelize(writeStatusList3));

      // There should be 1 SI records: 1 delete due to deletes3
      assertEquals(1, secondaryIndexRecords.size());
      // filter insert records from secondaryIndexRecords
      List<HoodieRecord> validSecondaryIndexRecords2 = new ArrayList<>();
      List<HoodieRecord> deletedSecondaryIndexRecords2 = new ArrayList<>();
      secondaryIndexRecords.forEach(record -> {
        populateValidAndDeletedSecondaryIndexRecords(record, deletedSecondaryIndexRecords2, validSecondaryIndexRecords2);
      });
      assertTrue(validSecondaryIndexRecords2.isEmpty());
      assertEquals(1, deletedSecondaryIndexRecords2.size());
      assertEquals(getRecordKeyFromSecondaryIndexKey(expectedDeletedIndexKeys.get(0)), getRecordKeyFromSecondaryIndexKey(deletedSecondaryIndexRecords2.get(0).getRecordKey()));

      // revive the deleted keys
      commitTime = client.startCommit();
      List<HoodieRecord> inserts4 = dataGen.generateSameKeyInserts(commitTime, deletes);
      List<String> expectedRevivedIndexKeys = inserts4.stream().map(TestMetadataUtilRLIandSIRecordGeneration::getSecondaryIndexKey).collect(Collectors.toList());
      List<HoodieRecord> records4 = new ArrayList<>();
      records4.addAll(inserts4);
      List<WriteStatus> writeStatusList4 = client.upsert(jsc.parallelize(records4, 1), commitTime).collect();
      assertNoWriteErrors(writeStatusList4);

      // assert SI
      String fourthCommitTime = commitTime;
      metaClient = HoodieTableMetaClient.reload(metaClient);
      metadata.reset();
      metadataView = new HoodieTableFileSystemView(metadata, metaClient, metaClient.getActiveTimeline());
      allWriteStats = writeStatusList4.stream().map(WriteStatus::getStat).collect(Collectors.toList());
      secondaryIndexRecords =
          convertWriteStatsToSecondaryIndexRecords(allWriteStats, fourthCommitTime, tableSchema, indexDefinition, metadataConfig, metaClient, engineContext, writeConfig).collectAsList();
      client.commit(fourthCommitTime, jsc.parallelize(writeStatusList4));

      // There should be 1 SI records: 1 insert due to inserts4
      assertEquals(1, secondaryIndexRecords.size());
      assertEquals(expectedRevivedIndexKeys.get(0), secondaryIndexRecords.get(0).getRecordKey());

      // generate update for the same key, but with the same rider value
      commitTime = client.startCommit();
      List<HoodieRecord> updates5 = dataGen.generateUpdatesWithTimestamp(fourthCommitTime, inserts4, Long.parseLong(commitTime));
      List<String> expectedUpdatedIndexKeys2 = updates5.stream().map(TestMetadataUtilRLIandSIRecordGeneration::getSecondaryIndexKey).collect(Collectors.toList());
      List<HoodieRecord> records5 = new ArrayList<>();
      records5.addAll(updates5);
      List<WriteStatus> writeStatusList5 = client.upsert(jsc.parallelize(records5, 1), commitTime).collect();
      assertNoWriteErrors(writeStatusList5);

      // assert SI
      String fifthCommitTime = commitTime;
      metaClient = HoodieTableMetaClient.reload(metaClient);
      metadata.reset();
      metadataView = new HoodieTableFileSystemView(metadata, metaClient, metaClient.getActiveTimeline());
      allWriteStats = writeStatusList5.stream().map(WriteStatus::getStat).collect(Collectors.toList());
      secondaryIndexRecords =
          convertWriteStatsToSecondaryIndexRecords(allWriteStats, fifthCommitTime, tableSchema, indexDefinition, metadataConfig, metaClient, engineContext, writeConfig).collectAsList();
      client.commit(fifthCommitTime, jsc.parallelize(writeStatusList5));

      // There should be 0 SI records because the secondary key field "rider" value has not changed.
      assertEquals(0, secondaryIndexRecords.size());

      // trigger compaction
      Option<String> compactionInstantOpt = client.scheduleCompaction(Option.empty());
      assertTrue(compactionInstantOpt.isPresent());
      HoodieWriteMetadata compactionWriteMetadata = client.compact(compactionInstantOpt.get());
      client.commitCompaction(compactionInstantOpt.get(), compactionWriteMetadata, Option.empty());
      assertTrue(metaClient.reloadActiveTimeline().filterCompletedInstants().containsInstant(compactionInstantOpt.get()));
      HoodieCommitMetadata compactionCommitMetadata = (HoodieCommitMetadata) compactionWriteMetadata.getCommitMetadata().get();
      // assert SI records
      metaClient = HoodieTableMetaClient.reload(metaClient);
      metadata.reset();
      metadataView = new HoodieTableFileSystemView(metadata, metaClient, metaClient.getActiveTimeline());
      allWriteStats = compactionCommitMetadata.getWriteStats();
      secondaryIndexRecords = convertWriteStatsToSecondaryIndexRecords(
          allWriteStats, compactionInstantOpt.get(), tableSchema, indexDefinition, metadataConfig, metaClient, engineContext, writeConfig).collectAsList();
      // Get valid and deleted secondary index records
      List<HoodieRecord> validSecondaryIndexRecords3 = new ArrayList<>();
      List<HoodieRecord> deletedSecondaryIndexRecords3 = new ArrayList<>();
      secondaryIndexRecords.forEach(record -> {
        populateValidAndDeletedSecondaryIndexRecords(record, deletedSecondaryIndexRecords3, validSecondaryIndexRecords3);
      });
      // There should 0 deleted records because compaction does not update any records.
      assertEquals(0, deletedSecondaryIndexRecords3.size());
      assertEquals(initialRecordsCount, dataGen.getNumExistingKeys(TRIP_EXAMPLE_SCHEMA));
    }
  }

  /**.
   * This tests that convertWriteStatsForNonNativeFormatToSecondaryIndexRecords correctly handles external files.
   * <p>
   * This test verifies:
   * - The method correctly processes external files (with _hudiext suffix)
   * - Insert SI records (isDeleted=false) are generated for new external files
   * - Delete SI records (isDeleted=true) are generated for replaced external files
   * - Record keys include full file paths in format: secondaryKey$filePath_position
   */
  @Test
  public void testSecondaryIndexRecordGenerationForInsertOverwriteNonNative() throws Exception {
    HoodieTableType tableType = HoodieTableType.COPY_ON_WRITE;
    cleanupClients();
    initMetaClient(tableType);
    cleanupTimelineService();
    initTimelineService();

    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);
    HoodieWriteConfig writeConfig = getConfigBuilder(HoodieFailedWritesCleaningPolicy.EAGER).build();
    String testPartition = "americas/united_states/san_francisco";
    String commitTime1 = "20260101000000";
    String commitTime2 = "20260102000000";

    // Setup index definition
    HoodieIndexDefinition indexDefinition = HoodieIndexDefinition.newBuilder()
        .withIndexName("secondary_index_idx_rider")
        .withIndexType(MetadataPartitionType.COLUMN_STATS.name())
        .withVersion(HoodieIndexVersion.getCurrentVersion(
            HoodieTableVersion.current(), "secondary_index_idx_rider"))
        .withIndexFunction("")
        .withSourceFields(Collections.singletonList("rider"))
        .withIndexOptions(Collections.emptyMap())
        .build();
    HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder()
        .enable(true)
        .withSecondaryIndexParallelism(2)
        .build();

    // Step 1: Write external parquet files that will be "removed"
    int initialRecordsCount = 10;
    List<HoodieRecord> removedRecords = dataGen.generateInsertsForPartition(commitTime1, initialRecordsCount, testPartition);

    List<String> removedFiles = new ArrayList<>();
    List<String> expectedRemovedRecordKeys = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      String fileName = "removed_file_" + i + ".parquet";
      String baseFilePath = Paths.get(basePath, testPartition, fileName).toString();
      writeExternalParquetFileWithTripData(baseFilePath, removedRecords.subList(i * 5, (i + 1) * 5));
      removedFiles.add(fileName);
      expectedRemovedRecordKeys.addAll(readExternalParquetFileWithRowPositions(baseFilePath));
    }

    // Step 2: Write external parquet files that are "added"
    int newRecordsCount = 5;
    List<HoodieRecord> addedRecords = dataGen.generateInsertsForPartition(commitTime2, newRecordsCount, testPartition);

    List<HoodieWriteStat> addedWriteStats = new ArrayList<>();
    List<String> expectedAddedRecordKeys = new ArrayList<>();
    String fileName = "added_file_0.parquet";
    String baseFilePath = Paths.get(basePath, testPartition, fileName).toString();
    addedWriteStats.add(writeExternalFileAndCreateWriteStat(basePath, testPartition, fileName, addedRecords, commitTime2));
    expectedAddedRecordKeys.addAll(readExternalParquetFileWithRowPositions(baseFilePath));

    // Step 3: Construct HoodieReplaceCommitMetadata
    Map<String, List<String>> partitionToReplaceFileIds = new HashMap<>();
    partitionToReplaceFileIds.put(testPartition, removedFiles);
    HoodieReplaceCommitMetadata replaceCommitMetadata = buildReplaceCommitMetadata(addedWriteStats, partitionToReplaceFileIds);

    // Step 4: Generate secondary index records for external files
    metaClient = HoodieTableMetaClient.reload(metaClient);
    Schema tableSchema = new Schema.Parser().parse(TRIP_EXAMPLE_SCHEMA);
    List<HoodieRecord> secondaryIndexRecords = convertWriteStatsForNonNativeFormatToSecondaryIndexRecords(
        engineContext, commitTime2, tableSchema, indexDefinition, metadataConfig,
        metaClient, writeConfig.getProps(), replaceCommitMetadata).collectAsList();

    // Step 5: Separate valid (insert) and deleted SI records
    List<HoodieRecord> validSIRecords = new ArrayList<>();
    List<HoodieRecord> deletedSIRecords = new ArrayList<>();
    secondaryIndexRecords.forEach(record -> {
      populateValidAndDeletedSecondaryIndexRecords(record, deletedSIRecords, validSIRecords);
    });

    // Verify counts
    assertEquals(newRecordsCount, validSIRecords.size(),
        "Should have insert SI records for all new records");
    assertEquals(initialRecordsCount, deletedSIRecords.size(),
        "Should have delete SI records for all replaced files");

    // Verify record keys match exactly (validates secondary key, file path, and position)
    Collections.sort(expectedAddedRecordKeys);
    Collections.sort(expectedRemovedRecordKeys);
    assertListEquality(expectedAddedRecordKeys, extractAndSortRecordKeys(validSIRecords));
    assertListEquality(expectedRemovedRecordKeys, extractAndSortRecordKeys(deletedSIRecords));
  }

  /**
   * Helper method to write external parquet files with trip data (for testing).
   */
  private void writeExternalParquetFileWithTripData(String filePath, List<HoodieRecord> records) throws Exception {
    Schema schema = new Schema.Parser().parse(TRIP_EXAMPLE_SCHEMA);
    ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(new Path(filePath))
        .withSchema(schema)
        .withCompressionCodec(CompressionCodecName.GZIP)
        .build();

    for (HoodieRecord record : records) {
      GenericRecord avroRecord = (GenericRecord) record.getData();
      writer.write(avroRecord);
    }
    writer.close();
  }

  /**
   * Reads an external parquet file and returns a list of expected record keys
   * in the format: secondaryKey$filePath_rowPosition
   *
   * @param baseFilePath Base file path (e.g., /path/to/file.parquet)
   * @return List of record keys matching the secondary index format
   */
  private List<String> readExternalParquetFileWithRowPositions(String baseFilePath) throws IOException {
    List<String> recordKeys = new ArrayList<>();

    ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(new Path(baseFilePath))
        .build();

    long position = 0;
    GenericRecord record;
    while ((record = reader.read()) != null) {
      String secondaryKey = record.get("rider").toString();
      // Format: secondaryKey$filePath_position (without _hudiext suffix)
      String recordKey = secondaryKey + "$" + baseFilePath + "_" + position;
      recordKeys.add(recordKey);
      position++;
    }
    reader.close();

    return recordKeys;
  }

  /**
   * Helper to write external parquet file and create HoodieWriteStat for it.
   */
  private HoodieWriteStat writeExternalFileAndCreateWriteStat(String basePath, String partition, String fileName,
                                                                List<HoodieRecord> records, String commitTime) throws Exception {
    String baseFilePath = Paths.get(basePath, partition, fileName).toString();
    writeExternalParquetFileWithTripData(baseFilePath, records);

    HoodieWriteStat writeStat = new HoodieWriteStat();
    String externalPath = ExternalFilePathUtil.appendCommitTimeAndExternalFileMarker(baseFilePath, commitTime);
    String externalFileName = FSUtils.getFileNameFromPath(externalPath);
    writeStat.setFileId(fileName);
    writeStat.setPath(partition + "/" + externalFileName);
    writeStat.setPartitionPath(partition);
    writeStat.setFileSizeInBytes(new java.io.File(baseFilePath).length());

    return writeStat;
  }

  /**
   * Helper to construct HoodieReplaceCommitMetadata with added and removed files.
   */
  private HoodieReplaceCommitMetadata buildReplaceCommitMetadata(List<HoodieWriteStat> addedWriteStats,
                                                                  Map<String, List<String>> partitionToRemovedFileIds) {
    HoodieReplaceCommitMetadata replaceCommitMetadata = new HoodieReplaceCommitMetadata();
    // Operation type not needed - external files auto-detected by _hudiext suffix
    addedWriteStats.forEach(stat -> replaceCommitMetadata.addWriteStat(stat.getPartitionPath(), stat));
    replaceCommitMetadata.setPartitionToReplaceFileIds(partitionToRemovedFileIds);
    replaceCommitMetadata.addMetadata(HoodieCommitMetadata.SCHEMA_KEY, TRIP_EXAMPLE_SCHEMA);
    return replaceCommitMetadata;
  }

  /**
   * Helper to extract and sort record keys from HoodieRecords.
   */
  private List<String> extractAndSortRecordKeys(List<HoodieRecord> records) {
    List<String> keys = records.stream()
        .map(HoodieRecord::getRecordKey)
        .sorted()
        .collect(Collectors.toList());
    return keys;
  }

  private static void populateValidAndDeletedSecondaryIndexRecords(HoodieRecord record, List<HoodieRecord> deletedSecondaryIndexRecords, List<HoodieRecord> validSecondaryIndexRecords) {
    if (record.isDelete(new DeleteContext(CollectionUtils.emptyProps(), HoodieMetadataRecord.getClassSchema()), CollectionUtils.emptyProps())) {
      deletedSecondaryIndexRecords.add(record);
    } else {
      validSecondaryIndexRecords.add(record);
    }
  }

  private static String getSecondaryIndexKey(HoodieRecord record) {
    return constructSecondaryIndexKey(((GenericRecord) record.getData()).get("rider").toString(), record.getRecordKey());
  }

  private void assertRLIandSIRecordGenerationAPIs(List<HoodieRecord> inserts3, List<HoodieRecord> updates3, List<HoodieRecord> deletes3,
                                                  List<WriteStatus> writeStatuses3, String finalCommitTime3, HoodieWriteConfig writeConfig) {
    List<String> expectedRLIInserts = inserts3.stream().map(record -> record.getKey().getRecordKey()).collect(Collectors.toList());
    List<String> expectedUpdates = updates3.stream().map(record -> record.getKey().getRecordKey()).collect(Collectors.toList());
    List<String> expectedRLIDeletes = deletes3.stream().map(record -> record.getKey().getRecordKey()).collect(Collectors.toList());
    List<String> expectedUpatesAndDeletes = new ArrayList<>(expectedRLIDeletes);
    expectedUpatesAndDeletes.addAll(expectedUpdates);

    // lets validate RLI record generation.
    List<String> actualInserts = new ArrayList<>();
    List<String> actualDeletes = new ArrayList<>();
    List<String> actualUpdatesAndDeletes = new ArrayList<>();
    generateRliRecordsAndAssert(writeStatuses3.stream().filter(writeStatus -> !FSUtils.isLogFile(FSUtils.getFileName(writeStatus.getStat().getPath(), writeStatus.getPartitionPath())))
        .collect(Collectors.toList()), Collections.emptyMap(), finalCommitTime3, writeConfig, actualInserts, actualDeletes);

    // lets also test HoodieTableMetadataUtil.getRecordKeys() for each individual log file touched as part of HoodieCommitMetadata.
    // lets test only deletes and also test both validat and deleted keys for log files.
    // we have disabled small file handling. And so, updates and deletes will definitely go into log files.
    String latestCommitTimestamp = metaClient.reloadActiveTimeline().getCommitsTimeline().lastInstant().get().requestedTime();
    Option<Schema> writerSchemaOpt = tryResolveSchemaForTable(metaClient);
    List<String> finalActualDeletes = actualDeletes;
    writeStatuses3.stream().filter(writeStatus -> FSUtils.isLogFile(FSUtils.getFileName(writeStatus.getStat().getPath(), writeStatus.getPartitionPath())))
        .forEach(writeStatus -> {
          try {
            HoodieWriteStat writeStat = writeStatus.getStat();
            StoragePath fullFilePath = new StoragePath(basePath, writeStat.getPath());
            // used for RLI
            HoodieReaderContext<?> readerContext = context.getReaderContextFactory(metaClient).getContext();
            finalActualDeletes.addAll(getRevivedAndDeletedKeysFromMergedLogs(metaClient, latestCommitTimestamp, Collections.singletonList(fullFilePath.toString()), writerSchemaOpt,
                Collections.singletonList(fullFilePath.toString()), writeStat.getPartitionPath(), readerContext, writeConfig.enableOptimizedLogBlocksScan()).getValue());

            // used in SI flow
            actualUpdatesAndDeletes.addAll(getRecordKeys(writeStat.getPartitionPath(), writeStat.getPrevCommit(), writeStat.getFileId(),
                Collections.singletonList(fullFilePath), metaClient, writerSchemaOpt, latestCommitTimestamp, writeConfig));
          } catch (IOException e) {
            throw new HoodieIOException("Failed w/ IOException ", e);
          }
        });

    assertListEquality(expectedRLIInserts, actualInserts);
    assertListEquality(expectedRLIDeletes, actualDeletes);
    assertListEquality(expectedUpatesAndDeletes, actualUpdatesAndDeletes);
  }

  @Test
  public void testReducedByKeysForRLIRecords() throws IOException {
    HoodieTableType tableType = HoodieTableType.COPY_ON_WRITE;
    cleanupClients();
    initMetaClient(tableType);
    cleanupTimelineService();
    initTimelineService();

    String commitTime = WriteClientTestUtils.createNewInstantTime();
    List<HoodieRecord> inserts = dataGen.generateInserts(commitTime, 100);
    List<HoodieRecord> deletes = dataGen.generateUniqueDeleteRecords(commitTime, 20);
    String randomFileId = UUID.randomUUID().toString() + "-0";
    List<String> deletedRecordKeys = deletes.stream().map(record -> record.getRecordKey()).collect(Collectors.toList());
    List<HoodieRecord> adjustedInserts = inserts.stream().filter(record -> !deletedRecordKeys.contains(record.getRecordKey())).collect(Collectors.toList());

    List<HoodieRecord> insertRecords =
        inserts.stream().map(record -> HoodieMetadataPayload.createRecordIndexUpdate(record.getRecordKey(), "abc", randomFileId, commitTime, 0))
            .collect(Collectors.toList());
    List<HoodieRecord> deleteRecords = inserts.stream().map(record -> HoodieMetadataPayload.createRecordIndexDelete(record.getRecordKey(), record.getPartitionPath(), false))
        .collect(Collectors.toList());

    List<HoodieRecord> recordsToTest = new ArrayList<>();
    recordsToTest.addAll(adjustedInserts);
    recordsToTest.addAll(deleteRecords);
    // happy paths. no dups. in and out are same.
    List<HoodieRecord> actualRecords = reduceByKeys(context.parallelize(recordsToTest, 2), 2, false).collectAsList();
    assertHoodieRecordListEquality(actualRecords, recordsToTest);

    // few records has both inserts and deletes.
    recordsToTest = new ArrayList<>();
    recordsToTest.addAll(insertRecords);
    recordsToTest.addAll(deleteRecords);
    actualRecords = reduceByKeys(context.parallelize(recordsToTest, 2), 2, false).collectAsList();
    List<HoodieRecord> expectedList = new ArrayList<>();
    expectedList.addAll(insertRecords);
    assertHoodieRecordListEquality(actualRecords, expectedList);

    // few deletes are duplicates. we are allowed to have duplicate deletes.
    recordsToTest = new ArrayList<>();
    recordsToTest.addAll(adjustedInserts);
    recordsToTest.addAll(deleteRecords);
    recordsToTest.addAll(deleteRecords.subList(0, 10));
    actualRecords = reduceByKeys(context.parallelize(recordsToTest, 2), 2, false).collectAsList();
    expectedList = new ArrayList<>();
    expectedList.addAll(adjustedInserts);
    expectedList.addAll(deleteRecords);
    assertHoodieRecordListEquality(actualRecords, expectedList);

    // test failure case. same record having 2 inserts should fail.
    recordsToTest = new ArrayList<>();
    recordsToTest.addAll(adjustedInserts);
    recordsToTest.addAll(adjustedInserts.subList(0, 5));
    try {
      reduceByKeys(context.parallelize(recordsToTest, 2), 2, false).collectAsList();
      fail("Should not have reached here");
    } catch (Exception e) {
      // expected. no-op
      assertTrue(e.getCause() instanceof HoodieIOException);
    }
  }

  private void assertHoodieRecordListEquality(List<HoodieRecord> actualList, List<HoodieRecord> expectedList) {
    DeleteContext deleteContext = new DeleteContext(CollectionUtils.emptyProps(), AVRO_SCHEMA).withReaderSchema(AVRO_SCHEMA);
    assertEquals(expectedList.size(), actualList.size());
    List<String> expectedInsertRecordKeys = expectedList.stream().filter(record -> !record.isDelete(deleteContext, CollectionUtils.emptyProps()))
        .map(record -> record.getRecordKey()).collect(Collectors.toList());
    List<String> expectedDeletedRecordKeys = expectedList.stream().filter(record -> record.isDelete(deleteContext, CollectionUtils.emptyProps()))
        .map(record -> record.getRecordKey()).collect(Collectors.toList());

    List<String> actualInsertRecordKeys = actualList.stream().filter(record -> !record.isDelete(deleteContext, CollectionUtils.emptyProps()))
        .map(record -> record.getRecordKey()).collect(Collectors.toList());
    List<String> actualDeletedRecordKeys = actualList.stream().filter(record -> record.isDelete(deleteContext, CollectionUtils.emptyProps()))
        .map(record -> record.getRecordKey()).collect(Collectors.toList());

    assertListEquality(expectedInsertRecordKeys, actualInsertRecordKeys);
    assertListEquality(expectedDeletedRecordKeys, actualDeletedRecordKeys);
  }

  private void assertListEquality(List<String> list1, List<String> list2) {
    Collections.sort(list1);
    Collections.sort(list2);
    assertEquals(list1, list2);
  }

  private static Option<Schema> tryResolveSchemaForTable(HoodieTableMetaClient dataTableMetaClient) {
    if (dataTableMetaClient.getCommitsTimeline().filterCompletedInstants().countInstants() == 0) {
      return Option.empty();
    }

    try {
      TableSchemaResolver schemaResolver = new TableSchemaResolver(dataTableMetaClient);
      return Option.of(schemaResolver.getTableAvroSchema());
    } catch (Exception e) {
      throw new HoodieException("Failed to get latest columns for " + dataTableMetaClient.getBasePath(), e);
    }
  }

  private void generateRliRecordsAndAssert(List<WriteStatus> writeStatuses, Map<String, String> fileIdToFileNameMapping, String commitTime,
                                           HoodieWriteConfig writeConfig, List<String> actualInserts,
                                           List<String> actualDeletes) {
    writeStatuses.forEach(writeStatus -> {
      if (!FSUtils.isLogFile(FSUtils.getFileName(writeStatus.getStat().getPath(), writeStatus.getPartitionPath()))) {
        // Fetch record keys for all
        String writeStatFileId = writeStatus.getFileId();
        if (!fileIdToFileNameMapping.isEmpty()) {
          assertEquals(writeStatus.getStat().getPrevBaseFile(), fileIdToFileNameMapping.get(writeStatFileId));
        }

        Iterator<HoodieRecord> rliRecordsItr = BaseFileRecordParsingUtils.generateRLIMetadataHoodieRecordsForBaseFile(metaClient.getBasePath().toString(), writeStatus.getStat(),
            writeConfig.getWritesFileIdEncoding(), commitTime, metaClient.getStorage(), writeConfig.isRecordLevelIndexEnabled());
        while (rliRecordsItr.hasNext()) {
          HoodieRecord rliRecord = rliRecordsItr.next();
          String key = rliRecord.getRecordKey();
          if (rliRecord.getData() instanceof EmptyHoodieRecordPayload) {
            actualDeletes.add(key);
          } else {
            actualInserts.add(key);
          }
        }
      }
    });
  }

  private void parseRecordKeysFromBaseFiles(List<WriteStatus> writeStatuses, Map<String, String> fileIdToFileNameMapping, String commitTime,
                                            HoodieWriteConfig writeConfig, List<String> actualInserts,
                                            List<String> actualDeletes, List<String> actualUpdates) {
    writeStatuses.forEach(writeStatus -> {
      if (!FSUtils.isLogFile(FSUtils.getFileName(writeStatus.getStat().getPath(), writeStatus.getPartitionPath()))) {
        // Fetch record keys for all
        String writeStatFileId = writeStatus.getFileId();
        if (!fileIdToFileNameMapping.isEmpty()) {
          assertEquals(writeStatus.getStat().getPrevBaseFile(), fileIdToFileNameMapping.get(writeStatFileId));
        }

        String partition = writeStatus.getStat().getPartitionPath();
        String latestFileName = FSUtils.getFileNameFromPath(writeStatus.getStat().getPath());

        Set<BaseFileRecordParsingUtils.RecordStatus> recordStatusSet = new HashSet<>();
        recordStatusSet.add(BaseFileRecordParsingUtils.RecordStatus.INSERT);
        recordStatusSet.add(BaseFileRecordParsingUtils.RecordStatus.UPDATE);
        recordStatusSet.add(BaseFileRecordParsingUtils.RecordStatus.DELETE);

        Map<BaseFileRecordParsingUtils.RecordStatus, List<String>> recordKeyMappings = BaseFileRecordParsingUtils.getRecordKeyStatuses(metaClient.getBasePath().toString(), partition, latestFileName,
            writeStatus.getStat().getPrevBaseFile(), storage, recordStatusSet);
        if (recordKeyMappings.containsKey(BaseFileRecordParsingUtils.RecordStatus.INSERT)) {
          actualInserts.addAll(recordKeyMappings.get(BaseFileRecordParsingUtils.RecordStatus.INSERT));
        }
        if (recordKeyMappings.containsKey(BaseFileRecordParsingUtils.RecordStatus.UPDATE)) {
          actualUpdates.addAll(recordKeyMappings.get(BaseFileRecordParsingUtils.RecordStatus.UPDATE));
        }
        if (recordKeyMappings.containsKey(BaseFileRecordParsingUtils.RecordStatus.DELETE)) {
          actualDeletes.addAll(recordKeyMappings.get(BaseFileRecordParsingUtils.RecordStatus.DELETE));
        }
      }
    });
  }

  Set<String> getRecordKeys(String partition, String baseInstantTime, String fileId, List<StoragePath> logFilePaths, HoodieTableMetaClient datasetMetaClient,
                            Option<Schema> writerSchemaOpt, String latestCommitTimestamp, HoodieWriteConfig writeConfig) throws IOException {
    if (writerSchemaOpt.isPresent()) {
      // read log file records without merging
      TypedProperties properties = new TypedProperties();
      // configure un-merged log file reader
      HoodieReaderContext readerContext = context.getReaderContextFactory(metaClient).getContext();
      HoodieFileGroupReader reader = HoodieFileGroupReader.newBuilder()
          .withReaderContext(readerContext)
          .withDataSchema(writerSchemaOpt.get())
          .withRequestedSchema(writerSchemaOpt.get())
          .withEmitDelete(true)
          .withPartitionPath(partition)
          .withLogFiles(logFilePaths.stream().map(HoodieLogFile::new))
          .withBaseFileOption(Option.empty())
          .withLatestCommitTime(latestCommitTimestamp)
          .withHoodieTableMetaClient(datasetMetaClient)
          .withProps(properties)
          .withEmitDelete(true)
          .withEnableOptimizedLogBlockScan(writeConfig.enableOptimizedLogBlocksScan())
          .build();
      Set<String> allRecordKeys = new HashSet<>();
      try (ClosableIterator<String> keysIterator = reader.getClosableKeyIterator()) {
        keysIterator.forEachRemaining(allRecordKeys::add);
      }
      return allRecordKeys;
    }
    return Collections.emptySet();
  }

  /**
   * This tests the new code path in convertMetadataToRecordIndexRecords that handles external base files written by non-native formats.
   * <p>
   * Test flow:
   * 1. Creates initial records in partition to simulate existing data files.
   * 3. Calls convertMetadataToRecordIndexRecords with replaced file IDs
   * 4. Verifies DELETE records are generated for all record keys in replaced files
   */
  @Test
  public void testRLIGenerationForInsertOverwriteExternalFormat() throws Exception {
    HoodieTableType tableType = HoodieTableType.COPY_ON_WRITE;
    cleanupClients();
    initMetaClient(tableType);
    cleanupTimelineService();
    initTimelineService();

    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);
    HoodieWriteConfig writeConfig = getConfigBuilder(HoodieFailedWritesCleaningPolicy.EAGER).build();
    String testPartition1 = "americas/brazil/sao_paulo";
    String testPartition2 = "americas/argentina/buenos_aires";
    String commitTime1 = "20260101000000";
    String commitTime2 = "20260102000000";

    // Step 1: Write external parquet files that will be "removed"
    int partition1RecordsCount = 10;
    int partition2RecordsCount = 5;
    List<HoodieRecord> removedRecordsP1 = dataGen.generateInsertsForPartition(commitTime1, partition1RecordsCount, testPartition1);
    List<HoodieRecord> removedRecordsP2 = dataGen.generateInsertsForPartition(commitTime1, partition2RecordsCount, testPartition2);

    Map<String, List<String>> partitionToRemovedFileIds = new HashMap<>();
    List<String> expectedDeletedRecordKeys = new ArrayList<>();

    // Write removed files for partition 1
    List<String> removedFilesP1 = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      String fileName = "removed_file_p1_" + i + ".parquet";
      String baseFilePath = Paths.get(basePath, testPartition1, fileName).toString();
      int startIdx = i * 5;
      int endIdx = Math.min((i + 1) * 5, removedRecordsP1.size());
      writeExternalParquetFileWithTripData(baseFilePath, removedRecordsP1.subList(startIdx, endIdx));

      // Add just the fileName (not the full external path) to partitionToReplaceFileIds
      removedFilesP1.add(fileName);

      // Read parquet and get expected RLI record keys (filePath_position format)
      expectedDeletedRecordKeys.addAll(readExternalParquetFileWithRowPositions(baseFilePath).stream()
          .map(key -> key.substring(key.indexOf('$') + 1))  // Extract filePath_position part
          .collect(Collectors.toList()));
    }
    partitionToRemovedFileIds.put(testPartition1, removedFilesP1);

    // Write removed files for partition 2
    List<String> removedFilesP2 = new ArrayList<>();
    String fileName = "removed_file_p2_0.parquet";
    String baseFilePath = Paths.get(basePath, testPartition2, fileName).toString();
    writeExternalParquetFileWithTripData(baseFilePath, removedRecordsP2);

    // Add just the fileName (not the full external path) to partitionToReplaceFileIds
    removedFilesP2.add(fileName);
    partitionToRemovedFileIds.put(testPartition2, removedFilesP2);

    // Read parquet and get expected RLI record keys (filePath_position format)
    expectedDeletedRecordKeys.addAll(readExternalParquetFileWithRowPositions(baseFilePath).stream()
        .map(key -> key.substring(key.indexOf('$') + 1))  // Extract filePath_position part
        .collect(Collectors.toList()));

    // Step 2: Write external parquet files that are "added"
    List<HoodieRecord> addedRecordsP1 = dataGen.generateInsertsForPartition(commitTime2, 5, testPartition1);
    List<HoodieRecord> addedRecordsP2 = dataGen.generateInsertsForPartition(commitTime2, 3, testPartition2);

    List<HoodieWriteStat> addedWriteStats = new ArrayList<>();
    List<String> expectedInsertedRecordKeys = new ArrayList<>();

    // Write added file for partition 1
    String addedFileName1 = "added_file_p1_0.parquet";
    String addedFilePath1 = Paths.get(basePath, testPartition1, addedFileName1).toString();
    addedWriteStats.add(writeExternalFileAndCreateWriteStat(basePath, testPartition1, addedFileName1, addedRecordsP1, commitTime2));
    expectedInsertedRecordKeys.addAll(readExternalParquetFileWithRowPositions(addedFilePath1).stream()
        .map(key -> key.substring(key.indexOf('$') + 1))  // Extract filePath_position part
        .collect(Collectors.toList()));

    // Write added file for partition 2
    String addedFileName2 = "added_file_p2_0.parquet";
    String addedFilePath2 = Paths.get(basePath, testPartition2, addedFileName2).toString();
    addedWriteStats.add(writeExternalFileAndCreateWriteStat(basePath, testPartition2, addedFileName2, addedRecordsP2, commitTime2));
    expectedInsertedRecordKeys.addAll(readExternalParquetFileWithRowPositions(addedFilePath2).stream()
        .map(key -> key.substring(key.indexOf('$') + 1))  // Extract filePath_position part
        .collect(Collectors.toList()));

    // Step 3: Construct HoodieReplaceCommitMetadata
    HoodieReplaceCommitMetadata replaceCommitMetadata = buildReplaceCommitMetadata(addedWriteStats, partitionToRemovedFileIds);

    // Step 4: Create metadata config with RLI enabled
    HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder()
        .enable(true)
        .withEnableGlobalRecordLevelIndex(true)
        .build();

    // Step 5: Call convertMetadataToRecordIndexRecords - this is the key test
    metaClient = HoodieTableMetaClient.reload(metaClient);
    List<HoodieRecord> rliRecords = convertMetadataToRecordIndexRecords(
        engineContext,
        replaceCommitMetadata,
        metadataConfig,
        metaClient,
        1,
        commitTime2,
        EngineType.SPARK,
        writeConfig.enableOptimizedLogBlocksScan()
    ).collectAsList();

    // Step 6: Separate INSERT and DELETE records
    List<HoodieRecord> insertRecords = new ArrayList<>();
    List<HoodieRecord> deleteRecords = new ArrayList<>();
    for (HoodieRecord record : rliRecords) {
      if (record.getData() instanceof EmptyHoodieRecordPayload) {
        deleteRecords.add(record);
      } else {
        insertRecords.add(record);
      }
    }

    // Verify counts
    int expectedNewRecords = addedRecordsP1.size() + addedRecordsP2.size(); // 5 + 3 = 8
    int expectedDeleteRecords = partition1RecordsCount + partition2RecordsCount; // 10 + 5 = 15
    assertEquals(expectedNewRecords, insertRecords.size(),
        "Should have INSERT RLI records for all new records");
    assertEquals(expectedDeleteRecords, deleteRecords.size(),
        "Should have DELETE RLI records for all replaced file records");

    // Verify record keys match expected (validates file path and position)
    Collections.sort(expectedInsertedRecordKeys);
    Collections.sort(expectedDeletedRecordKeys);
    assertListEquality(expectedInsertedRecordKeys, extractAndSortRecordKeys(insertRecords));
    assertListEquality(expectedDeletedRecordKeys, extractAndSortRecordKeys(deleteRecords));
  }
}