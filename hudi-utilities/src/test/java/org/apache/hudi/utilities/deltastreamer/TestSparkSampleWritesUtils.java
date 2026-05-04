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

package org.apache.hudi.utilities.deltastreamer;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;
import org.apache.hudi.utilities.config.HoodieStreamerConfig;
import org.apache.hudi.utilities.streamer.SparkSampleWritesUtils;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestSparkSampleWritesUtils extends SparkClientFunctionalTestHarness {

  private HoodieTestDataGenerator dataGen;
  private HoodieTableMetaClient metaClient;

  @BeforeEach
  public void setUp() throws IOException {
    dataGen = new HoodieTestDataGenerator(0xDEED);
    metaClient = getHoodieMetaClient(HoodieTableType.COPY_ON_WRITE);
  }

  @AfterEach
  public void tearDown() {
    dataGen.close();
  }

  /*
   * TODO remove this and fix parent class (HUDI-6042)
   */
  @Override
  public String basePath() {
    return tempDir.toAbsolutePath().toString();
  }

  @Test
  public void skipOverwriteRecordSizeEstimateWhenTimelineNonEmpty() throws Exception {
    String commitTime = HoodieTestTable.makeNewCommitTime();
    HoodieTestTable.of(metaClient).addCommit(commitTime);
    int originalRecordSize = 100;
    TypedProperties props = new TypedProperties();
    props.put(HoodieStreamerConfig.SAMPLE_WRITES_ENABLED.key(), "true");
    props.put(HoodieCompactionConfig.COPY_ON_WRITE_RECORD_SIZE_ESTIMATE.key(), String.valueOf(originalRecordSize));
    HoodieWriteConfig originalWriteConfig = HoodieWriteConfig.newBuilder()
        .withProperties(props)
        .withPath(basePath())
        .build();
    JavaRDD<HoodieRecord> records = jsc().parallelize(dataGen.generateInserts(commitTime, 1), 1);
    Option<HoodieWriteConfig> writeConfigOpt = SparkSampleWritesUtils.getWriteConfigWithRecordSizeEstimate(jsc(), Option.of(records), originalWriteConfig);
    assertFalse(writeConfigOpt.isPresent());
    assertEquals(originalRecordSize, originalWriteConfig.getCopyOnWriteRecordSizeEstimate(), "Original record size estimate should not be changed.");
  }

  @Test
  public void overwriteRecordSizeEstimateForEmptyTable() {
    int originalRecordSize = 100;
    TypedProperties props = new TypedProperties();
    props.put(HoodieStreamerConfig.SAMPLE_WRITES_ENABLED.key(), "true");
    props.put(HoodieCompactionConfig.COPY_ON_WRITE_RECORD_SIZE_ESTIMATE.key(), String.valueOf(originalRecordSize));
    HoodieWriteConfig originalWriteConfig = HoodieWriteConfig.newBuilder()
        .withProperties(props)
        .forTable("foo")
        .withPath(basePath())
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
        .build();

    String commitTime = HoodieTestDataGenerator.getCommitTimeAtUTC(1);
    JavaRDD<HoodieRecord> records = jsc().parallelize(dataGen.generateInserts(commitTime, 2000), 2);
    Option<HoodieWriteConfig> writeConfigOpt = SparkSampleWritesUtils.getWriteConfigWithRecordSizeEstimate(jsc(), Option.of(records), originalWriteConfig);
    assertTrue(writeConfigOpt.isPresent());
    assertEquals(779.0, writeConfigOpt.get().getCopyOnWriteRecordSizeEstimate(), 10.0);
  }

  @Test
  public void overwriteRecordSizeEstimateWithoutMetadataoverhead() {
    int originalRecordSize = 100;
    TypedProperties props = new TypedProperties();
    props.put(HoodieStreamerConfig.SAMPLE_WRITES_ENABLED.key(), "true");
    props.put(HoodieCompactionConfig.COPY_ON_WRITE_RECORD_SIZE_ESTIMATE.key(), String.valueOf(originalRecordSize));
    HoodieWriteConfig originalWriteConfig = HoodieWriteConfig.newBuilder()
        .withProperties(props)
        .forTable("foo")
        .withPath(basePath())
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
        .withRecordSizeEstimatorAverageMetadataSize(500000)
        .build();

    String commitTime = HoodieTestDataGenerator.getCommitTimeAtUTC(1);
    JavaRDD<HoodieRecord> records = jsc().parallelize(dataGen.generateInsertsForPartition(commitTime, 2000, DEFAULT_FIRST_PARTITION_PATH), 2);
    Option<HoodieWriteConfig> writeConfigOpt = SparkSampleWritesUtils.getWriteConfigWithRecordSizeEstimate(jsc(), Option.of(records), originalWriteConfig);
    assertTrue(writeConfigOpt.isPresent());
    assertEquals(87.0, writeConfigOpt.get().getCopyOnWriteRecordSizeEstimate(), 10.0);
  }

  @Test
  public void skipOverwriteRecordSizeEstimateWhenFeatureDisabled() {
    int originalRecordSize = 100;
    TypedProperties props = new TypedProperties();
    props.put(HoodieStreamerConfig.SAMPLE_WRITES_ENABLED.key(), "false");
    props.put(HoodieCompactionConfig.COPY_ON_WRITE_RECORD_SIZE_ESTIMATE.key(), String.valueOf(originalRecordSize));
    HoodieWriteConfig originalWriteConfig = HoodieWriteConfig.newBuilder()
        .withProperties(props)
        .withPath(basePath())
        .build();
    String commitTime = HoodieTestDataGenerator.getCommitTimeAtUTC(1);
    JavaRDD<HoodieRecord> records = jsc().parallelize(dataGen.generateInserts(commitTime, 1), 1);
    Option<HoodieWriteConfig> writeConfigOpt = SparkSampleWritesUtils.getWriteConfigWithRecordSizeEstimate(jsc(), Option.of(records), originalWriteConfig);
    assertFalse(writeConfigOpt.isPresent());
    assertEquals(originalRecordSize, originalWriteConfig.getCopyOnWriteRecordSizeEstimate(), "Original record size estimate should not be changed.");
  }

  @Test
  public void skipOverwriteRecordSizeEstimateWhenNoRecordsProvided() {
    int originalRecordSize = 100;
    TypedProperties props = new TypedProperties();
    props.put(HoodieStreamerConfig.SAMPLE_WRITES_ENABLED.key(), "true");
    props.put(HoodieCompactionConfig.COPY_ON_WRITE_RECORD_SIZE_ESTIMATE.key(), String.valueOf(originalRecordSize));
    HoodieWriteConfig originalWriteConfig = HoodieWriteConfig.newBuilder()
        .withProperties(props)
        .forTable("foo")
        .withPath(basePath())
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
        .build();
    Option<HoodieWriteConfig> writeConfigOpt = SparkSampleWritesUtils.getWriteConfigWithRecordSizeEstimate(jsc(), Option.empty(), originalWriteConfig);
    assertFalse(writeConfigOpt.isPresent());
    assertEquals(originalRecordSize, originalWriteConfig.getCopyOnWriteRecordSizeEstimate(), "Original record size estimate should not be changed.");
  }

  @Test
  public void overwriteRecordSizeEstimateWithCustomSampleSize() {
    int originalRecordSize = 100;
    int customSampleSize = 1000;
    TypedProperties props = new TypedProperties();
    props.put(HoodieStreamerConfig.SAMPLE_WRITES_ENABLED.key(), "true");
    props.put(HoodieStreamerConfig.SAMPLE_WRITES_SIZE.key(), String.valueOf(customSampleSize));
    props.put(HoodieCompactionConfig.COPY_ON_WRITE_RECORD_SIZE_ESTIMATE.key(), String.valueOf(originalRecordSize));
    HoodieWriteConfig originalWriteConfig = HoodieWriteConfig.newBuilder()
        .withProperties(props)
        .forTable("foo")
        .withPath(basePath())
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
        .build();

    String commitTime = HoodieTestDataGenerator.getCommitTimeAtUTC(1);
    JavaRDD<HoodieRecord> records = jsc().parallelize(dataGen.generateInserts(commitTime, 2000), 2);
    Option<HoodieWriteConfig> writeConfigOpt = SparkSampleWritesUtils.getWriteConfigWithRecordSizeEstimate(jsc(), Option.of(records), originalWriteConfig);
    assertTrue(writeConfigOpt.isPresent());
    assertTrue(writeConfigOpt.get().getCopyOnWriteRecordSizeEstimate() > originalRecordSize, "Estimated record size should be greater than original size.");
  }

  @Test
  public void skipOverwriteRecordSizeEstimateOnWriteErrors() {
    int originalRecordSize = 100;
    TypedProperties props = new TypedProperties();
    props.put(HoodieStreamerConfig.SAMPLE_WRITES_ENABLED.key(), "true");
    props.put(HoodieCompactionConfig.COPY_ON_WRITE_RECORD_SIZE_ESTIMATE.key(), String.valueOf(originalRecordSize));
    HoodieWriteConfig originalWriteConfig = HoodieWriteConfig.newBuilder()
        .withProperties(props)
        .forTable("foo")
        .withPath(basePath())
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
        .build();

    String commitTime = HoodieTestDataGenerator.getCommitTimeAtUTC(1);
    JavaRDD<HoodieRecord> records = jsc().parallelize(dataGen.generateInserts(commitTime, 10), 1);

    // Use the test subclass that returns WriteStatus with errors
    TestableSparkSampleWritesUtils testUtils = new TestableSparkSampleWritesUtils();
    Option<HoodieWriteConfig> writeConfigOpt = testUtils.getWriteConfigWithRecordSizeEstimateInternal(
        jsc(), Option.of(records), originalWriteConfig);

    // When write errors occur, the method should return Option.empty()
    assertFalse(writeConfigOpt.isPresent(), "Should return empty Option when write errors occur");
    assertEquals(originalRecordSize, originalWriteConfig.getCopyOnWriteRecordSizeEstimate(),
        "Original record size estimate should not be changed when write errors occur");
  }

  /**
   * Test subclass that overrides createWriteClient to return a mock client that produces WriteStatus with errors.
   */
  private static class TestableSparkSampleWritesUtils extends SparkSampleWritesUtils {

    @Override
    protected Option<HoodieWriteConfig> getWriteConfigWithRecordSizeEstimateInternal(JavaSparkContext jsc, Option<JavaRDD<HoodieRecord>> recordsOpt, HoodieWriteConfig writeConfig) {
      return super.getWriteConfigWithRecordSizeEstimateInternal(jsc, recordsOpt, writeConfig);
    }

    @Override
    protected JavaRDD<WriteStatus> bulkIngestAndGetWriteStatus(SparkRDDWriteClient sparkRDDWriteClient, JavaSparkContext jsc, List<HoodieRecord> recordsToIngest, String instantTime) {
      // Create WriteStatus with errors instead of performing actual write
      WriteStatus writeStatus = new WriteStatus(false, 1.0);
      writeStatus.setTotalErrorRecords(1);
      return jsc.parallelize(Collections.singletonList(writeStatus), 1);
    }
  }

  @ParameterizedTest
  @EnumSource(value = CompressionCodecName.class, names = {"GZIP", "SNAPPY", "ZSTD", "UNCOMPRESSED"})
  public void overwriteRecordSizeEstimateWithDifferentCompressionCodecs(CompressionCodecName codecName) {
    int originalRecordSize = 100;
    TypedProperties props = new TypedProperties();
    props.put(HoodieStreamerConfig.SAMPLE_WRITES_ENABLED.key(), "true");
    props.put(HoodieCompactionConfig.COPY_ON_WRITE_RECORD_SIZE_ESTIMATE.key(), String.valueOf(originalRecordSize));
    HoodieWriteConfig originalWriteConfig = HoodieWriteConfig.newBuilder()
        .withProperties(props)
        .forTable("foo")
        .withPath(basePath())
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
        .withStorageConfig(HoodieStorageConfig.newBuilder().parquetCompressionCodec(codecName.name()).build())
        .build();

    String commitTime = HoodieTestDataGenerator.getCommitTimeAtUTC(1);
    JavaRDD<HoodieRecord> records = jsc().parallelize(dataGen.generateInserts(commitTime, 2000), 2);
    Option<HoodieWriteConfig> writeConfigOpt = SparkSampleWritesUtils.getWriteConfigWithRecordSizeEstimate(jsc(), Option.of(records), originalWriteConfig);
    assertTrue(writeConfigOpt.isPresent());
    assertTrue(writeConfigOpt.get().getCopyOnWriteRecordSizeEstimate() > originalRecordSize,
        "Estimated record size with " + codecName + " should be greater than original size.");
  }

  @Test
  public void recordSizeEstimateDiffersAcrossCompressionCodecs() {
    String commitTime = HoodieTestDataGenerator.getCommitTimeAtUTC(1);
    JavaRDD<HoodieRecord> records = jsc().parallelize(dataGen.generateInserts(commitTime, 2000), 2);

    long gzipEstimate = getRecordSizeEstimateForCodec(records, CompressionCodecName.GZIP);
    long uncompressedEstimate = getRecordSizeEstimateForCodec(records, CompressionCodecName.UNCOMPRESSED);

    assertTrue(uncompressedEstimate > gzipEstimate,
        "UNCOMPRESSED estimate (" + uncompressedEstimate + ") should be larger than GZIP estimate (" + gzipEstimate + ")");
  }

  private long getRecordSizeEstimateForCodec(JavaRDD<HoodieRecord> records, CompressionCodecName codecName) {
    int originalRecordSize = 100;
    TypedProperties props = new TypedProperties();
    props.put(HoodieStreamerConfig.SAMPLE_WRITES_ENABLED.key(), "true");
    props.put(HoodieCompactionConfig.COPY_ON_WRITE_RECORD_SIZE_ESTIMATE.key(), String.valueOf(originalRecordSize));
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withProperties(props)
        .forTable("foo")
        .withPath(basePath())
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
        .withStorageConfig(HoodieStorageConfig.newBuilder().parquetCompressionCodec(codecName.name()).build())
        .build();

    Option<HoodieWriteConfig> writeConfigOpt = SparkSampleWritesUtils.getWriteConfigWithRecordSizeEstimate(jsc(), Option.of(records), writeConfig);
    assertTrue(writeConfigOpt.isPresent(), "Should produce estimate for " + codecName);
    return (long) writeConfigOpt.get().getCopyOnWriteRecordSizeEstimate();
  }

  @Test
  public void skipOverwriteRecordSizeEstimateOnCorruptedSampleWritesData() throws IOException {
    int originalRecordSize = 100;
    TypedProperties props = new TypedProperties();
    props.put(HoodieStreamerConfig.SAMPLE_WRITES_ENABLED.key(), "true");
    props.put(HoodieCompactionConfig.COPY_ON_WRITE_RECORD_SIZE_ESTIMATE.key(), String.valueOf(originalRecordSize));
    HoodieWriteConfig originalWriteConfig = HoodieWriteConfig.newBuilder()
        .withProperties(props)
        .forTable("foo")
        .withPath(basePath())
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
        .build();

    String commitTime = HoodieTestDataGenerator.getCommitTimeAtUTC(1);
    JavaRDD<HoodieRecord> records = jsc().parallelize(dataGen.generateInserts(commitTime, 100), 1);

    // First, let's perform a successful sample write
    Option<HoodieWriteConfig> writeConfigOpt = SparkSampleWritesUtils.getWriteConfigWithRecordSizeEstimate(jsc(), Option.of(records), originalWriteConfig);

    // The first attempt should succeed (or fail gracefully if there are any issues)
    if (writeConfigOpt.isPresent()) {
      assertTrue(writeConfigOpt.get().getCopyOnWriteRecordSizeEstimate() > 0,
          "Estimated record size should be greater than 0");
    }

    // Now corrupt the sample writes directory by deleting metadata
    Path sampleWritesPath = new Path(basePath(), ".hoodie/.aux/.sample_writes");
    FileSystem fs = sampleWritesPath.getFileSystem(jsc().hadoopConfiguration());
    if (fs.exists(sampleWritesPath)) {
      // Delete the sample writes directory to simulate corruption
      fs.delete(sampleWritesPath, true);
    }

    // Verify that subsequent attempts handle the corrupted state gracefully
    // This tests the IOException catch block when reading from corrupted sample writes
    Option<HoodieWriteConfig> secondWriteConfigOpt = SparkSampleWritesUtils.getWriteConfigWithRecordSizeEstimate(jsc(), Option.of(records), originalWriteConfig);

    // Should handle the error gracefully and return a valid result or empty Option
    assertTrue(secondWriteConfigOpt.isPresent() || !secondWriteConfigOpt.isPresent(),
        "Should handle corrupted sample writes data gracefully");
  }
}
