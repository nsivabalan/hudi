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

package org.apache.hudi.functional

import org.apache.hudi.{DataSourceWriteOptions, SparkDatasetMixin}
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.client.SparkRDDWriteClient
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.config.{HoodieMetadataConfig, TypedProperties}
import org.apache.hudi.common.data.HoodieListData
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.{HoodieRecord, HoodieRecordGlobalLocation, HoodieTableType}
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.testutils.{HoodieTestDataGenerator, InProcessTimeGenerator}
import org.apache.hudi.common.testutils.HoodieTestDataGenerator.recordsToStrings
import org.apache.hudi.common.util.{Option => HOption}
import org.apache.hudi.config.{HoodieCompactionConfig, HoodieIndexConfig, HoodieWriteConfig}
import org.apache.hudi.functional.TestRecordLevelIndex.TestPartitionedRecordLevelIndexTestCase
import org.apache.hudi.index.HoodieIndex.IndexType.RECORD_LEVEL_INDEX
import org.apache.hudi.index.record.HoodieRecordIndex
import org.apache.hudi.metadata.{HoodieBackedTableMetadata, HoodieTableMetadataUtil, MetadataPartitionType}
import org.apache.hudi.storage.StoragePath
import org.apache.hudi.table.action.compact.strategy.UnBoundedCompactionStrategy

import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.functions.lit
import org.junit.jupiter.api.{Tag, Test}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue, fail}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{Arguments, EnumSource, MethodSource, ValueSource}

import java.util
import java.util.Collections
import java.util.stream.Collectors

import scala.collection.JavaConverters
import scala.collection.JavaConverters._

@Tag("functional")
class TestRecordLevelIndex extends RecordLevelIndexTestBase with SparkDatasetMixin {
  private class testRecordLevelIndexHolder {
    var bulkRecordKeys: java.util.List[String] = null
    var options: Map[String, String] = null
    var recordKeys: java.util.List[String] = null
    var newRecordKeys: java.util.List[String] = null
  }

  @Test
  def testRLIInitializationForMorWithOnlyBaseFiles(): Unit = {
    // Test the optimized RLI initialization path for MOR tables that only have base files (no log files)
    // This should use the faster readRecordKeysFromBaseFiles path instead of readRecordKeysFromFileSliceSnapshot
    val hudiOpts = commonOpts ++ Map(
      DataSourceWriteOptions.TABLE_TYPE.key -> HoodieTableType.MERGE_ON_READ.name()
    )

    // First insert - creates base files
    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite)

    // Second insert - creates more base files (INSERT doesn't create log files in MOR)
    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append)

    // Verify no log files exist by checking commit metadata
    val timeline = metaClient.reloadActiveTimeline().getCommitsTimeline.filterCompletedInstants()
    val lastInstant = timeline.lastInstant().get()
    val commitMetadata = timeline.readCommitMetadata(lastInstant)
    val hasLogFiles = commitMetadata.getPartitionToWriteStats.values().asScala
      .flatMap(_.asScala)
      .exists(writeStat => writeStat.getPath.contains(".log"))

    assertTrue(!hasLogFiles, "MOR table should not have log files after INSERT operations")

    // Now drop and re-initialize RLI - this should use the optimized base-file-only path
    val writeConfig = getWriteConfig(hudiOpts)
    metadataWriter(writeConfig).dropMetadataPartitions(Collections.singletonList(MetadataPartitionType.RECORD_INDEX.getPartitionPath))
    // dropMetadataPartitions only deletes physical files; the table config must also be updated so
    // that the next write detects RLI as uninitialized and re-initializes it
    metaClient.getTableConfig.setMetadataPartitionState(metaClient, MetadataPartitionType.RECORD_INDEX.getPartitionPath, false)
    assertEquals(0, getFileGroupCountForRecordIndex(writeConfig))

    // Re-enable RLI - initialization should use the optimized path for base-file-only file slices
    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append)

    // Validate the RLI was correctly initialized
    validateDataAndRecordIndices(hudiOpts)
  }

  @Test
  def testRLIInitializationForMorWithLogFiles(): Unit = {
    // Test that MOR tables with log files still use the merge path (not the optimized path)
    val hudiOpts = commonOpts ++ Map(
      DataSourceWriteOptions.TABLE_TYPE.key -> HoodieTableType.MERGE_ON_READ.name(),
      HoodieCompactionConfig.INLINE_COMPACT.key() -> "false",
      HoodieCompactionConfig.PARQUET_SMALL_FILE_LIMIT.key() -> "0"
    )

    // First insert - creates base files
    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite)

    // Upsert - creates log files
    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append)

    // Verify log files exist by checking commit metadata
    val timeline = metaClient.reloadActiveTimeline().getCommitsTimeline.filterCompletedInstants()
    val lastInstant = timeline.lastInstant().get()
    val commitMetadata = timeline.readCommitMetadata(lastInstant)
    val hasLogFiles = commitMetadata.getPartitionToWriteStats.values().asScala
      .flatMap(_.asScala)
      .exists(writeStat => writeStat.getPath.contains(".log"))

    assertTrue(hasLogFiles, "MOR table should have log files after UPSERT operations")

    // Drop and re-initialize RLI - this should use the merge path due to log files
    val writeConfig = getWriteConfig(hudiOpts)
    metadataWriter(writeConfig).dropMetadataPartitions(Collections.singletonList(MetadataPartitionType.RECORD_INDEX.getPartitionPath))
    // dropMetadataPartitions only deletes physical files; the table config must also be updated so
    // that the next write detects RLI as uninitialized and re-initializes it
    metaClient.getTableConfig.setMetadataPartitionState(metaClient, MetadataPartitionType.RECORD_INDEX.getPartitionPath, false)
    assertEquals(0, getFileGroupCountForRecordIndex(writeConfig))

    // Re-enable RLI - initialization should use the merge path for file slices with log files
    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append)

    // Validate the RLI was correctly initialized with merged data
    validateDataAndRecordIndices(hudiOpts)
  }

  @Test
  def testRLIInitializationForMorGlobalIndex(): Unit = {
    val tableType = HoodieTableType.MERGE_ON_READ
    val hudiOpts = commonOpts + (DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name()) +
      (HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_MIN_FILE_GROUP_COUNT_PROP.key -> "1") +
      (HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_MAX_FILE_GROUP_COUNT_PROP.key -> "1") +
      (HoodieIndexConfig.INDEX_TYPE.key -> "RECORD_INDEX") +
      (HoodieIndexConfig.RECORD_INDEX_UPDATE_PARTITION_PATH_ENABLE.key -> "true") -
      HoodieMetadataConfig.RECORD_LEVEL_INDEX_ENABLE_PROP.key

    val dataGen1 = HoodieTestDataGenerator.createTestGeneratorFirstPartition()
    val dataGen2 = HoodieTestDataGenerator.createTestGeneratorSecondPartition()

    // batch1 inserts
    val instantTime1 = metaClient.createNewInstantTime(false)
    val latestBatch = recordsToStrings(dataGen1.generateInserts(instantTime1, 5)).asScala.toSeq
    var operation = INSERT_OPERATION_OPT_VAL
    val latestBatchDf = spark.read.json(spark.sparkContext.parallelize(latestBatch, 1))
    latestBatchDf.cache()
    latestBatchDf.write.format("org.apache.hudi")
      .options(hudiOpts)
      .mode(SaveMode.Overwrite)
      .save(basePath)
    val deletedDf1 = calculateMergedDf(latestBatchDf, operation, true)
    deletedDf1.cache()

    // batch2. upsert. update few records to 2nd partition from partition1 and insert a few to partition2.
    val instantTime2 = metaClient.createNewInstantTime(false)

    val latestBatch2_1 = recordsToStrings(dataGen1.generateUniqueUpdates(instantTime2, 3)).asScala.toSeq
    val latestBatchDf2_1 = spark.read.json(spark.sparkContext.parallelize(latestBatch2_1, 1))
    val latestBatchDf2_2 = latestBatchDf2_1.withColumn("partition", lit(HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH))
      .withColumn("partition_path", lit(HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH))
    val latestBatch2_3 = recordsToStrings(dataGen2.generateInserts(instantTime2, 2)).asScala.toSeq
    val latestBatchDf2_3 = spark.read.json(spark.sparkContext.parallelize(latestBatch2_3, 1))
    val latestBatchDf2Final = latestBatchDf2_3.union(latestBatchDf2_2)
    latestBatchDf2Final.cache()
    latestBatchDf2Final.write.format("org.apache.hudi")
      .options(hudiOpts)
      .mode(SaveMode.Append)
      .save(basePath)
    operation = UPSERT_OPERATION_OPT_VAL
    val deletedDf2 = calculateMergedDf(latestBatchDf2Final, operation, true)
    deletedDf2.cache()

    val hudiOpts2 = commonOpts + (DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name()) +
      (HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_MIN_FILE_GROUP_COUNT_PROP.key -> "1") +
      (HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_MAX_FILE_GROUP_COUNT_PROP.key -> "1") +
      (HoodieIndexConfig.INDEX_TYPE.key -> "RECORD_INDEX") +
      (HoodieIndexConfig.RECORD_INDEX_UPDATE_PARTITION_PATH_ENABLE.key -> "true") +
      (HoodieMetadataConfig.RECORD_LEVEL_INDEX_ENABLE_PROP.key -> "true")

    val instantTime3 = metaClient.createNewInstantTime(false)
    // batch3. updates to partition2
    val latestBatch3 = recordsToStrings(dataGen2.generateUniqueUpdates(instantTime3, 2)).asScala.toSeq
    val latestBatchDf3 = spark.read.json(spark.sparkContext.parallelize(latestBatch3, 1))
    latestBatchDf3.cache()
    latestBatchDf3.write.format("org.apache.hudi")
      .options(hudiOpts2)
      .mode(SaveMode.Append)
      .save(basePath)
    val deletedDf3 = calculateMergedDf(latestBatchDf3, operation, true)
    deletedDf3.cache()
    validateDataAndRecordIndices(hudiOpts2, deletedDf3)
  }

  def testRecordLevelIndex(tableType: HoodieTableType, streamingWriteEnabled: Boolean, holder: testRecordLevelIndexHolder): Unit = {
    val dataGen = new HoodieTestDataGenerator();
    val inserts = dataGen.generateInserts("001", 5)
    val latestBatchDf = toDataset(spark, inserts)
    val insertDf = latestBatchDf.withColumn("data_partition_path", lit("partition1")).union(latestBatchDf.withColumn("data_partition_path", lit("partition2")))
    val options = Map(HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
      DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name(),
      RECORDKEY_FIELD.key -> "_row_key",
      PARTITIONPATH_FIELD.key -> "data_partition_path",
      HoodieTableConfig.ORDERING_FIELDS.key -> "timestamp",
      HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP.key()-> "false",
      HoodieMetadataConfig.RECORD_LEVEL_INDEX_ENABLE_PROP.key() -> "true",
      HoodieMetadataConfig.STREAMING_WRITE_ENABLED.key() -> streamingWriteEnabled.toString,
      HoodieCompactionConfig.INLINE_COMPACT.key() -> "false",
      HoodieIndexConfig.INDEX_TYPE.key() -> RECORD_LEVEL_INDEX.name())
    holder.options = options
    insertDf.write.format("hudi")
      .options(options)
      .mode(SaveMode.Overwrite)
      .save(basePath)
    assertEquals(10, spark.read.format("hudi").load(basePath).count())
    val props = TypedProperties.fromMap(JavaConverters.mapAsJavaMapConverter(options).asJava)
    val writeConfig = HoodieWriteConfig.newBuilder()
      .withProps(props)
      .withPath(basePath)
      .build()
    var metadata = metadataWriter(writeConfig).getTableMetadata
    val recordKeys = inserts.asScala.map(i => i.getRecordKey).asJava.stream().collect(Collectors.toList())
    holder.recordKeys = recordKeys
    var partition1Locations = readRecordIndex(metadata, recordKeys, HOption.of("partition1"))
    assertEquals(5, partition1Locations.size)
    var partition2Locations = readRecordIndex(metadata, recordKeys, HOption.of("partition2"))
    assertEquals(5, partition2Locations.size)
    var df = spark.read.format("hudi").load(basePath).collect()
    validateDFWithLocations(df, partition1Locations, "partition1")
    validateDFWithLocations(df, partition2Locations, "partition2")

    val newDeletes =  dataGen.generateUpdates("004", 1)
    val updates =  dataGen.generateUniqueUpdates("002", 3)
    val lowerOrderingValue = 1L
    updates.addAll(dataGen.generateUniqueDeleteRecords("002", 2, lowerOrderingValue))
    val nextBatchDf = toDataset(spark, updates)
    val updateDf = nextBatchDf.withColumn("data_partition_path", lit("partition1"))

    updateDf.write.format("hudi")
      .options(options)
      .option(DataSourceWriteOptions.OPERATION.key(), UPSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)
    assertEquals(10, spark.read.format("hudi").load(basePath).count())
    partition1Locations = readRecordIndex(metadata, recordKeys, HOption.of("partition1"))
    assertEquals(5, partition1Locations.size)
    partition2Locations = readRecordIndex(metadata, recordKeys, HOption.of("partition2"))
    assertEquals(5, partition2Locations.size)
    df = spark.read.format("hudi").load(basePath).collect()
    validateDFWithLocations(df, partition1Locations, "partition1")
    validateDFWithLocations(df, partition2Locations, "partition2")

    val newInserts =  dataGen.generateInserts("003", 3)
    val newInsertBatchDf = toDataset(spark, newInserts)
    val newInsertDf = newInsertBatchDf.withColumn("data_partition_path", lit("partition2")).union(newInsertBatchDf.withColumn("data_partition_path", lit("partition3")))
    newInsertDf.write.format("hudi")
      .options(options)
      .option(DataSourceWriteOptions.OPERATION.key(), UPSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)
    assertEquals(16, spark.read.format("hudi").load(basePath).count())
    metadata = metadataWriter(writeConfig).getTableMetadata
    partition1Locations = readRecordIndex(metadata, recordKeys, HOption.of("partition1"))
    assertEquals(5, partition1Locations.size)
    partition2Locations = readRecordIndex(metadata, recordKeys, HOption.of("partition2"))
    assertEquals(5, partition2Locations.size)
    df = spark.read.format("hudi").load(basePath).collect()
    validateDFWithLocations(df, partition1Locations, "partition1")
    validateDFWithLocations(df, partition2Locations, "partition2")

    val newRecordKeys = newInserts.asScala.map(i => i.getRecordKey).asJava.stream().collect(Collectors.toList())
    holder.newRecordKeys = newRecordKeys
    partition1Locations = readRecordIndex(metadata, newRecordKeys, HOption.of("partition1"))
    assertEquals(0, partition1Locations.size)
    partition2Locations = readRecordIndex(metadata, newRecordKeys, HOption.of("partition2"))
    assertEquals(3, partition2Locations.size)
    var partition3Locations = readRecordIndex(metadata, newRecordKeys, HOption.of("partition3"))
    assertEquals(3, partition3Locations.size)
    validateDFWithLocations(df, partition3Locations, "partition3")

    val newDeletesBatchDf = toDataset(spark, newDeletes)
    val newDeletesDf = newDeletesBatchDf.withColumn("data_partition_path", lit("partition1"))
    newDeletesDf.write.format("hudi")
      .options(options)
      .option(DataSourceWriteOptions.OPERATION.key(), DELETE_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)
    assertEquals(15, spark.read.format("hudi").load(basePath).count())
    metadata = metadataWriter(writeConfig).getTableMetadata
    partition1Locations = readRecordIndex(metadata, recordKeys, HOption.of("partition1"))
    assertEquals(4, partition1Locations.size)
    partition2Locations = readRecordIndex(metadata, recordKeys, HOption.of("partition2"))
    assertEquals(5, partition2Locations.size)
    df = spark.read.format("hudi").load(basePath).collect()
    validateDFWithLocations(df, partition1Locations, "partition1")
    validateDFWithLocations(df, partition2Locations, "partition2")

    assertFalse(partition1Locations.contains(newDeletes.get(0).getRecordKey))
    assertTrue(partition2Locations.contains(newDeletes.get(0).getRecordKey))
    partition1Locations = readRecordIndex(metadata, newRecordKeys, HOption.of("partition1"))
    assertEquals(0, partition1Locations.size)
    partition2Locations = readRecordIndex(metadata, newRecordKeys, HOption.of("partition2"))
    assertEquals(3, partition2Locations.size)
    partition3Locations = readRecordIndex(metadata, newRecordKeys, HOption.of("partition3"))
    assertEquals(3, partition3Locations.size)
    validateDFWithLocations(df, partition2Locations, "partition2")
    validateDFWithLocations(df, partition3Locations, "partition3")

    val bulkInserts = dataGen.generateInserts("005", 5)
    val bulkInsertDf = toDataset(spark, bulkInserts)
    val bulkInsertPartitionedDf = bulkInsertDf.withColumn("data_partition_path", lit("partition0"))

    // Use bulk_insert operation explicitly
    bulkInsertPartitionedDf.write.format("hudi")
      .options(options)
      .option(DataSourceWriteOptions.OPERATION.key(), DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)

    val bulkRecordKeys = bulkInserts.asScala.map(_.getRecordKey).asJava
    holder.bulkRecordKeys = bulkRecordKeys
    metadata = metadataWriter(writeConfig).getTableMetadata
    val partition0Locations = readRecordIndex(metadata, bulkRecordKeys, HOption.of("partition0"))
    assertEquals(5, partition0Locations.size)
    df = spark.read.format("hudi").load(basePath).collect()
    validateDFWithLocations(df, partition0Locations, "partition0")

    metaClient = HoodieTableMetaClient.reload(metaClient)
    assertTrue(HoodieRecordIndex.isPartitioned(metaClient.getIndexMetadata.get().getIndexDefinitions.get(HoodieTableMetadataUtil.PARTITION_NAME_RECORD_INDEX)))
  }

  @ParameterizedTest
  @MethodSource(Array("testArgsForPartitionedRecordLevelIndex"))
  def testPartitionedRecordLevelIndexRollback(testCase: TestPartitionedRecordLevelIndexTestCase): Unit = {
    val holder = new testRecordLevelIndexHolder
    testRecordLevelIndex(testCase.tableType, testCase.streamingWriteEnabled, holder)
    val writeConfig = getWriteConfig(holder.options)
    new SparkRDDWriteClient(new HoodieSparkEngineContext(jsc), writeConfig)
      .rollback(metaClient.getActiveTimeline.lastInstant().get().requestedTime())
    val metadata = metadataWriter(writeConfig).getTableMetadata
    try {
      val partition0Locations = readRecordIndex(metadata, holder.bulkRecordKeys, HOption.of("partition0"))
      fail("rollback happened, so partition should be deleted")
    } catch {
      case t: Throwable => assertTrue(t.isInstanceOf[ArithmeticException])
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testPartitionedRecordLevelIndexCompact(streamingWriteEnabled: Boolean): Unit = {
    val holder = new testRecordLevelIndexHolder
    testRecordLevelIndex(HoodieTableType.MERGE_ON_READ, streamingWriteEnabled, holder)
    assertEquals("deltacommit", metaClient.getActiveTimeline.lastInstant().get().getAction)
    val writeConfig = getWriteConfig(holder.options)
    var metadata = metadataWriter(writeConfig).getTableMetadata
    doAllAssertions(holder, metadata)
    val writeClient = new SparkRDDWriteClient(new HoodieSparkEngineContext(jsc), writeConfig)
    val timeOpt = writeClient.scheduleCompaction(HOption.empty())
    assertTrue(timeOpt.isPresent)
    writeClient.compact(timeOpt.get())
    metaClient.reloadActiveTimeline()
    assertEquals("compaction", metaClient.getActiveTimeline.lastInstant().get().getAction)
    metadata = metadataWriter(writeConfig).getTableMetadata
    doAllAssertions(holder, metadata)
  }

  @ParameterizedTest
  @MethodSource(Array("testArgsForPartitionedRecordLevelIndex"))
  def testPartitionedRecordLevelIndexCluster(testCase: TestPartitionedRecordLevelIndexTestCase): Unit = {
    val holder = new testRecordLevelIndexHolder
    testRecordLevelIndex(testCase.tableType, testCase.streamingWriteEnabled, holder)
    assertEquals(if (testCase.tableType.equals(HoodieTableType.MERGE_ON_READ)) "deltacommit" else "commit",
      metaClient.getActiveTimeline.lastInstant().get().getAction)
    val writeConfig = getWriteConfig(holder.options ++ Map(HoodieWriteConfig.AVRO_SCHEMA_STRING.key() -> HoodieTestDataGenerator.AVRO_SCHEMA.toString))
    var metadata = metadataWriter(writeConfig).getTableMetadata
    doAllAssertions(holder, metadata)
    val writeClient = new SparkRDDWriteClient(new HoodieSparkEngineContext(jsc), writeConfig)
    val timeOpt = writeClient.scheduleClustering(HOption.empty())
    assertTrue(timeOpt.isPresent)
    writeClient.cluster(timeOpt.get())
    metaClient.reloadActiveTimeline()
    assertEquals("replacecommit", metaClient.getActiveTimeline.lastInstant().get().getAction)
    metadata = metadataWriter(writeConfig).getTableMetadata
    doAllAssertions(holder, metadata)
  }

  private def validateDFWithLocations(df: Array[Row], locations: Map[String, HoodieRecordGlobalLocation],
                                       partition: String): Unit = {
    var count: Int = 0
    for (row <- df) {
      val recordKey = row.getString(2)
      locations.get(recordKey).foreach { loc =>
        if (partition == row.getString(3)) {
          count += 1
          assertEquals(row.getString(3), loc.getPartitionPath)
          assertEquals(FSUtils.getFileId(row.getString(4)), loc.getFileId)
        }
      }
    }
    assertEquals(locations.size, count)
  }

  private def doAllAssertions(holder: testRecordLevelIndexHolder, metadata: HoodieBackedTableMetadata): Unit = {
    val df = spark.read.format("hudi").load(basePath).collect()
    var partition0Locations = readRecordIndex(metadata, holder.recordKeys, HOption.of("partition0"))
    assertEquals(0, partition0Locations.size)
    var partition1Locations = readRecordIndex(metadata, holder.recordKeys, HOption.of("partition1"))
    assertEquals(4, partition1Locations.size)
    validateDFWithLocations(df, partition1Locations, "partition1")
    var partition2Locations = readRecordIndex(metadata, holder.recordKeys, HOption.of("partition2"))
    assertEquals(5, partition2Locations.size)
    validateDFWithLocations(df, partition2Locations, "partition2")
    var partition3Locations = readRecordIndex(metadata, holder.recordKeys, HOption.of("partition3"))
    assertEquals(0, partition3Locations.size)

    partition0Locations = readRecordIndex(metadata, holder.newRecordKeys, HOption.of("partition0"))
    assertEquals(0, partition0Locations.size)
    partition1Locations = readRecordIndex(metadata, holder.newRecordKeys, HOption.of("partition1"))
    assertEquals(0, partition1Locations.size)
    partition2Locations = readRecordIndex(metadata, holder.newRecordKeys, HOption.of("partition2"))
    assertEquals(3, partition2Locations.size)
    validateDFWithLocations(df, partition2Locations, "partition2")
    partition3Locations = readRecordIndex(metadata, holder.newRecordKeys, HOption.of("partition3"))
    assertEquals(3, partition3Locations.size)
    validateDFWithLocations(df, partition3Locations, "partition3")

    partition0Locations = readRecordIndex(metadata, holder.bulkRecordKeys, HOption.of("partition0"))
    assertEquals(5, partition0Locations.size)
    validateDFWithLocations(df, partition0Locations, "partition0")
    partition1Locations = readRecordIndex(metadata, holder.bulkRecordKeys, HOption.of("partition1"))
    assertEquals(0, partition1Locations.size)
    partition2Locations = readRecordIndex(metadata, holder.bulkRecordKeys, HOption.of("partition2"))
    assertEquals(0, partition2Locations.size)
    partition3Locations = readRecordIndex(metadata, holder.bulkRecordKeys, HOption.of("partition3"))
    assertEquals(0, partition3Locations.size)
  }

  @ParameterizedTest
  @MethodSource(Array("testArgsForPartitionedRecordLevelIndex"))
  def testPartitionedRecordLevelIndexInitializationBasic(testCase: TestPartitionedRecordLevelIndexTestCase): Unit = {
    testPartitionedRecordLevelIndexInitialization(testCase.tableType, testCase.streamingWriteEnabled, failAndDoRollback = false, compact = false, cluster = false)
  }

  @ParameterizedTest
  @MethodSource(Array("testArgsForPartitionedRecordLevelIndex"))
  def testPartitionedRecordLevelIndexInitializationRollback(testCase: TestPartitionedRecordLevelIndexTestCase): Unit = {
    testPartitionedRecordLevelIndexInitialization(testCase.tableType, testCase.streamingWriteEnabled, failAndDoRollback = true, compact = false, cluster = false)
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testPartitionedRecordLevelIndexInitializationCompact(streamingWriteEnabled: Boolean): Unit = {
    testPartitionedRecordLevelIndexInitialization(HoodieTableType.MERGE_ON_READ, streamingWriteEnabled, failAndDoRollback = false, compact = true, cluster = false)
  }

  @ParameterizedTest
  @MethodSource(Array("testArgsForPartitionedRecordLevelIndex"))
  def testPartitionedRecordLevelIndexInitializationCluster(testCase: TestPartitionedRecordLevelIndexTestCase): Unit = {
    testPartitionedRecordLevelIndexInitialization(testCase.tableType, testCase.streamingWriteEnabled, failAndDoRollback = false, compact = false, cluster = true)
  }

  def testPartitionedRecordLevelIndexInitialization(tableType: HoodieTableType,
                                                    streamingWriteEnabled: Boolean,
                                                    failAndDoRollback: Boolean,
                                                    compact: Boolean,
                                                    cluster: Boolean): Unit = {
    initMetaClient(tableType)
    val dataGen = new HoodieTestDataGenerator()
    val inserts = dataGen.generateInserts("001", 5)
    val latestBatchDf = toDataset(spark, inserts)
    val insertDf = latestBatchDf.withColumn("data_partition_path", lit("partition1")).union(latestBatchDf.withColumn("data_partition_path", lit("partition2")))
    val options = Map(HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
      DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name(),
      RECORDKEY_FIELD.key -> "_row_key",
      PARTITIONPATH_FIELD.key -> "data_partition_path",
      HoodieTableConfig.ORDERING_FIELDS.key -> "timestamp",
      HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP.key()-> "false",
      HoodieMetadataConfig.SECONDARY_INDEX_ENABLE_PROP.key() -> "false",
      HoodieMetadataConfig.STREAMING_WRITE_ENABLED.key() -> streamingWriteEnabled.toString,
      HoodieCompactionConfig.INLINE_COMPACT.key() -> "false",
      HoodieIndexConfig.INDEX_TYPE.key() -> RECORD_LEVEL_INDEX.name())
    insertDf.write.format("hudi")
      .options(options)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    assertEquals(10, spark.read.format("hudi").load(basePath).count())

    val updates =  dataGen.generateUniqueUpdates("002", 3)
    val nextBatchDf = toDataset(spark, updates)
    val updateDf = nextBatchDf.withColumn("data_partition_path", lit("partition1"))
    updateDf.write.format("hudi")
      .options(options)
      .option(DataSourceWriteOptions.OPERATION.key(), UPSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)
    assertEquals(10, spark.read.format("hudi").load(basePath).count())
    metaClient.reloadActiveTimeline()
    val tableSchemaResolver = new TableSchemaResolver(metaClient)
    val latestTableSchemaFromCommitMetadata = tableSchemaResolver.getTableAvroSchemaFromLatestCommit(false)

    if (failAndDoRollback) {
      val updatesToFail =  dataGen.generateUniqueUpdates("003", 3)
      val batchToFailDf = toDataset(spark, updatesToFail)
      val failDf = batchToFailDf.withColumn("data_partition_path", lit("partition1")).union(batchToFailDf.withColumn("data_partition_path", lit("partition3")))
      failDf.write.format("hudi")
        .options(options)
        .option(DataSourceWriteOptions.OPERATION.key(), UPSERT_OPERATION_OPT_VAL)
        .mode(SaveMode.Append)
        .save(basePath)
      assertEquals(13, spark.read.format("hudi").load(basePath).count())

      metaClient.reloadActiveTimeline()
      val lastInstant = metaClient.getActiveTimeline.lastInstant().get()
      assertTrue(storage.deleteFile(new StoragePath(metaClient.getTimelinePath, metaClient.getInstantFileNameGenerator.getFileName(lastInstant))))
      assertEquals(10, spark.read.format("hudi").load(basePath).count())

      // rollback
      val writeConfig = HoodieWriteConfig.newBuilder()
        .withProps(TypedProperties.fromMap(JavaConverters
          .mapAsJavaMapConverter(options ++ Map(HoodieWriteConfig.AVRO_SCHEMA_STRING.key() -> latestTableSchemaFromCommitMetadata.get().toString)).asJava))
        .withPath(basePath)
        .build()
      new SparkRDDWriteClient(new HoodieSparkEngineContext(jsc), writeConfig)
        .rollback(lastInstant.requestedTime())
    }

    if (compact) {
      assertEquals("deltacommit", metaClient.getActiveTimeline.lastInstant().get().getAction)
      val writeConfig = getWriteConfig(options ++
        Map(HoodieCompactionConfig.COMPACTION_STRATEGY.key() -> classOf[UnBoundedCompactionStrategy].getName,
          HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key() -> "1"))
      val writeClient = new SparkRDDWriteClient(new HoodieSparkEngineContext(jsc), writeConfig)
      val timeOpt = writeClient.scheduleCompaction(HOption.empty())
      assertTrue(timeOpt.isPresent)
      writeClient.compact(timeOpt.get())
      metaClient.reloadActiveTimeline()
      assertEquals("compaction", metaClient.getActiveTimeline.lastInstant().get().getAction)
    }

    if (cluster) {
      assertEquals(if (tableType.equals(HoodieTableType.MERGE_ON_READ)) "deltacommit" else "commit",
        metaClient.getActiveTimeline.lastInstant().get().getAction)
      val writeConfig = getWriteConfig(options ++ Map(HoodieWriteConfig.AVRO_SCHEMA_STRING.key() -> HoodieTestDataGenerator.AVRO_SCHEMA.toString))
      val writeClient = new SparkRDDWriteClient(new HoodieSparkEngineContext(jsc), writeConfig)
      val timeOpt = writeClient.scheduleClustering(HOption.empty())
      assertTrue(timeOpt.isPresent)
      writeClient.cluster(timeOpt.get())
      metaClient.reloadActiveTimeline()
      assertEquals("replacecommit", metaClient.getActiveTimeline.lastInstant().get().getAction)
    }

    //init mdt
    val updateOptions = options ++ Map(HoodieMetadataConfig.RECORD_LEVEL_INDEX_ENABLE_PROP.key() -> "true",
      HoodieWriteConfig.AVRO_SCHEMA_STRING.key() -> latestTableSchemaFromCommitMetadata.get().toString)
    val props = TypedProperties.fromMap(JavaConverters.mapAsJavaMapConverter(updateOptions).asJava)
    val writeConfig = HoodieWriteConfig.newBuilder()
      .withProps(props)
      .withPath(basePath)
      .build()
    val metadata = metadataWriter(writeConfig).getTableMetadata
    val recordKeys = inserts.asScala.map(i => i.getRecordKey).asJava.stream().collect(Collectors.toList())
    val partition1Locations = readRecordIndex(metadata, recordKeys, HOption.of("partition1"))
    assertEquals(5, partition1Locations.size)
    val partition2Locations = readRecordIndex(metadata, recordKeys, HOption.of("partition2"))
    assertEquals(5, partition2Locations.size)
    val df = spark.read.format("hudi").load(basePath).collect()
    validateDFWithLocations(df, partition1Locations, "partition1")
    validateDFWithLocations(df, partition2Locations, "partition2")

    metaClient = HoodieTableMetaClient.reload(metaClient)
    assertTrue(HoodieRecordIndex.isPartitioned(metaClient.getIndexMetadata.get().getIndexDefinitions.get(HoodieTableMetadataUtil.PARTITION_NAME_RECORD_INDEX)))
  }

  def readRecordIndex(metadata: HoodieBackedTableMetadata, recordKeys: java.util.List[String], dataTablePartition: HOption[String]): Map[String, HoodieRecordGlobalLocation] = {
    metadata.readRecordIndexLocationsWithKeys(HoodieListData.eager(recordKeys), dataTablePartition)
      .collectAsList().asScala.map(p => p.getKey -> p.getValue).toMap
  }

  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testRLIForDeletesWithHoodieIsDeletedColumn(tableType: HoodieTableType): Unit = {
    val hudiOpts = commonOpts + (DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name()) +
      (HoodieIndexConfig.INDEX_TYPE.key -> "RECORD_INDEX") +
      (HoodieIndexConfig.RECORD_INDEX_UPDATE_PARTITION_PATH_ENABLE.key -> "false")
    val insertDf = doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite)
    insertDf.cache()

    val instantTime = InProcessTimeGenerator.createNewInstantTime()
    // Issue two deletes, where one has an older ordering value that should be ignored
    val deletedRecords = dataGen.generateUniqueDeleteRecords(instantTime, 1)
    val inputRecords = new util.ArrayList[HoodieRecord[_]](deletedRecords)
    val lowerOrderingValue = 1L
    inputRecords.addAll(dataGen.generateUniqueDeleteRecords(instantTime, 1, lowerOrderingValue))
    val deleteBatch = recordsToStrings(inputRecords).asScala
    val deleteDf = spark.read.json(spark.sparkContext.parallelize(deleteBatch.toSeq, 1))
    deleteDf.cache()
    val recordKeyToDelete = deleteDf.collectAsList().get(0).getAs("_row_key").asInstanceOf[String]
    deleteDf.write.format("hudi")
      .options(hudiOpts)
      .mode(SaveMode.Append)
      .save(basePath)
    val prevDf = mergedDfList.last
    mergedDfList = mergedDfList :+ prevDf.filter(row => row.getAs("_row_key").asInstanceOf[String] != recordKeyToDelete)
    validateDataAndRecordIndices(hudiOpts, spark.read.json(spark.sparkContext.parallelize(recordsToStrings(deletedRecords).asScala.toSeq, 1)))
    deleteDf.unpersist()
  }

  @Test
  def testPartitionedRecordLevelIndexWithHiveStylePartitioningAndDotInPartitionField(): Unit = {
    initMetaClient(HoodieTableType.COPY_ON_WRITE)
    val dataGen = new HoodieTestDataGenerator()
    val inserts = dataGen.generateInserts("001", 10)
    val insertDf = toDataset(spark, inserts)

    // Use fare.currency as partition field to test dots in partition field names with Hive-style partitioning
    val options = Map(HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
      DataSourceWriteOptions.TABLE_TYPE.key -> HoodieTableType.COPY_ON_WRITE.name(),
      RECORDKEY_FIELD.key -> "_row_key",
      PARTITIONPATH_FIELD.key -> "fare.currency",
      HoodieTableConfig.ORDERING_FIELDS.key -> "timestamp",
      HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP.key() -> "false",
      HoodieMetadataConfig.RECORD_LEVEL_INDEX_ENABLE_PROP.key() -> "true",
      HoodieMetadataConfig.STREAMING_WRITE_ENABLED.key() -> "false",
      HoodieCompactionConfig.INLINE_COMPACT.key() -> "false",
      HoodieIndexConfig.INDEX_TYPE.key() -> RECORD_LEVEL_INDEX.name(),
      DataSourceWriteOptions.HIVE_STYLE_PARTITIONING.key() -> "true")

    insertDf.write.format("hudi")
      .options(options)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    assertEquals(10, spark.read.format("hudi").load(basePath).count())

    val props = TypedProperties.fromMap(JavaConverters.mapAsJavaMapConverter(options).asJava)
    val writeConfig = HoodieWriteConfig.newBuilder()
      .withProps(props)
      .withPath(basePath)
      .build()

    val metadata = metadataWriter(writeConfig).getTableMetadata
    val recordKeys = inserts.asScala.map(insert => insert.getRecordKey).asJava.stream().collect(Collectors.toList())

    // Derive expected partition path and count from the actual written data
    val writtenDf = spark.read.format("hudi").load(basePath)
    val partitionCounts = writtenDf.groupBy("_hoodie_partition_path").count().collect()
      .map(row => (row.getString(0), row.getLong(1))).toMap
    // Expect exactly one partition with a dot in the field name (hive-style)
    assertEquals(1, partitionCounts.size)
    val (partitionPath, expectedCount) = partitionCounts.head
    assertTrue(partitionPath.startsWith("fare.currency="))

    val usdPartitionLocations = readRecordIndex(metadata, recordKeys, HOption.of(partitionPath))
    assertEquals(expectedCount, usdPartitionLocations.size)

    val df = writtenDf.collect()
    if (usdPartitionLocations.nonEmpty) {
      validateDFWithLocations(df, usdPartitionLocations, partitionPath)
    }

    // Verify partitioned RLI is enabled
    metaClient = HoodieTableMetaClient.reload(metaClient)
    val indexDefinitions = metaClient.getIndexMetadata.get().getIndexDefinitions
    val rliIndexDefinition = indexDefinitions.get(HoodieTableMetadataUtil.PARTITION_NAME_RECORD_INDEX)
    assertTrue(HoodieRecordIndex.isPartitioned(rliIndexDefinition))

    // Do an update to ensure ongoing writes work correctly with dot in partition field
    val updates = dataGen.generateUniqueUpdates("002", 2)
    val updateDf = toDataset(spark, updates)
    updateDf.write.format("hudi")
      .options(options)
      .option(DataSourceWriteOptions.OPERATION.key(), UPSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)

    assertEquals(expectedCount, spark.read.format("hudi").load(basePath).count())

    // Verify record index still works after updates
    val metadataAfterUpdate = metadataWriter(writeConfig).getTableMetadata
    val usdLocationsAfterUpdate = readRecordIndex(metadataAfterUpdate, recordKeys, HOption.of(partitionPath))
    assertEquals(expectedCount, usdLocationsAfterUpdate.size)
  }
}

object TestRecordLevelIndex {

  case class TestPartitionedRecordLevelIndexTestCase(tableType: HoodieTableType, streamingWriteEnabled: Boolean)

  def testArgsForPartitionedRecordLevelIndex: java.util.stream.Stream[Arguments] = {
    java.util.stream.Stream.of(
      Arguments.arguments(TestPartitionedRecordLevelIndexTestCase(HoodieTableType.COPY_ON_WRITE, streamingWriteEnabled = true)),
      Arguments.arguments(TestPartitionedRecordLevelIndexTestCase(HoodieTableType.COPY_ON_WRITE, streamingWriteEnabled = false)),
      Arguments.arguments(TestPartitionedRecordLevelIndexTestCase(HoodieTableType.MERGE_ON_READ, streamingWriteEnabled = true)),
      Arguments.arguments(TestPartitionedRecordLevelIndexTestCase(HoodieTableType.MERGE_ON_READ, streamingWriteEnabled = false))
    )
  }
}
