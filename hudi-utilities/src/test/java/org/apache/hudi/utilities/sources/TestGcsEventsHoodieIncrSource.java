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

package org.apache.hudi.utilities.sources;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.testutils.SchemaTestUtil;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.common.util.collection.Triple;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;
import org.apache.hudi.utilities.schema.FilebasedSchemaProvider;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.helpers.CloudDataFetcher;
import org.apache.hudi.utilities.sources.helpers.CloudObjectMetadata;
import org.apache.hudi.utilities.sources.helpers.IncrSourceHelper;
import org.apache.hudi.utilities.sources.helpers.QueryRunner;
import org.apache.hudi.utilities.sources.helpers.gcs.GcsObjectMetadataFetcher;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.apache.hudi.utilities.sources.helpers.IncrSourceHelper.MissingCheckpointStrategy.READ_UPTO_LATEST_COMMIT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestGcsEventsHoodieIncrSource extends SparkClientFunctionalTestHarness {

  private static final Schema GCS_METADATA_SCHEMA = SchemaTestUtil.getSchemaFromResource(
      TestGcsEventsHoodieIncrSource.class, "/streamer-config/gcs-metadata.avsc", true);

  private ObjectMapper mapper = new ObjectMapper();

  @TempDir
  protected java.nio.file.Path tempDir;

  @Mock
  GcsObjectMetadataFetcher gcsObjectMetadataFetcher;

  @Mock
  CloudDataFetcher gcsObjectDataFetcher;

  @Mock
  QueryRunner queryRunner;

  protected Option<SchemaProvider> schemaProvider;
  private HoodieTableMetaClient metaClient;
  private JavaSparkContext jsc;

  private static final Logger LOG = LoggerFactory.getLogger(TestGcsEventsHoodieIncrSource.class);

  @BeforeEach
  public void setUp() throws IOException {
    metaClient = getHoodieMetaClient(hadoopConf(), basePath());
    jsc = JavaSparkContext.fromSparkContext(spark().sparkContext());
    String schemaFilePath = TestGcsEventsHoodieIncrSource.class.getClassLoader().getResource("schema/sample_gcs_data.avsc").getPath();
    TypedProperties props = new TypedProperties();
    props.put("hoodie.deltastreamer.schemaprovider.source.schema.file", schemaFilePath);
    props.put("hoodie.deltastreamer.schema.provider.class.name", FilebasedSchemaProvider.class.getName());
    this.schemaProvider = Option.of(new FilebasedSchemaProvider(props, jsc));
    MockitoAnnotations.initMocks(this);
  }

  @Override
  public String basePath() {
    return tempDir.toAbsolutePath().toUri().toString();
  }

  @Test
  public void shouldNotFindNewDataIfCommitTimeOfWriteAndReadAreEqual() throws IOException {
    String commitTimeForWrites = "1";
    String commitTimeForReads = commitTimeForWrites;

    Pair<String, List<HoodieRecord>> inserts = writeGcsMetadataRecords(commitTimeForWrites);

    readAndAssert(READ_UPTO_LATEST_COMMIT, Option.of(commitTimeForReads), 100L, 0, inserts.getKey());

    verify(gcsObjectMetadataFetcher, times(0)).getGcsObjectMetadata(Mockito.any(), Mockito.any(),
            anyBoolean());
    verify(gcsObjectDataFetcher, times(0)).getCloudObjectDataDF(
        Mockito.any(), Mockito.any(), Mockito.any(), eq(schemaProvider));
  }

  @Test
  public void shouldFetchDataIfCommitTimeForReadsLessThanForWrites() throws IOException {
    String commitTimeForWrites = "2";
    String commitTimeForReads = "1";

    Pair<String, List<HoodieRecord>> inserts = writeGcsMetadataRecords(commitTimeForWrites);
    List<CloudObjectMetadata> cloudObjectMetadataList = Arrays.asList(
        new CloudObjectMetadata("data-file-1.json", 1),
        new CloudObjectMetadata("data-file-2.json", 1));
    when(gcsObjectMetadataFetcher.getGcsObjectMetadata(Mockito.any(), Mockito.any(), anyBoolean())).thenReturn(cloudObjectMetadataList);

    List<Row> recs = Arrays.asList(
        new GenericRow(new String[] {"1", "Hello 1"}),
        new GenericRow(new String[] {"2", "Hello 2"}),
        new GenericRow(new String[] {"3", "Hello 3"}),
        new GenericRow(new String[] {"4", "Hello 4"})
    );
    StructType schema = new StructType(new StructField[] {
        DataTypes.createStructField("id", DataTypes.StringType, true),
        DataTypes.createStructField("text", DataTypes.StringType, true)
    });
    Dataset<Row> rows = spark().createDataFrame(recs, schema);
    List<Triple<String, Long, String>> filePathSizeAndCommitTime = new ArrayList<>();
    // Add file paths and sizes to the list
    filePathSizeAndCommitTime.add(Triple.of("path/to/file1.json", 100L, "1"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/file2.json", 150L, "1"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/file3.json", 200L, "1"));
    Dataset<Row> inputDs = generateDataset(filePathSizeAndCommitTime);

    when(gcsObjectDataFetcher.getCloudObjectDataDF(Mockito.any(), eq(cloudObjectMetadataList), Mockito.any(),
        eq(schemaProvider))).thenReturn(Option.of(rows));
    when(queryRunner.run(Mockito.any())).thenReturn(inputDs);

    readAndAssert(READ_UPTO_LATEST_COMMIT, Option.of(commitTimeForReads), 100L, 4, "1#path/to/file1.json");

    verify(gcsObjectMetadataFetcher, times(1)).getGcsObjectMetadata(Mockito.any(), Mockito.any(),
            anyBoolean());
    verify(gcsObjectDataFetcher, times(1)).getCloudObjectDataDF(Mockito.any(),
        eq(cloudObjectMetadataList), Mockito.any(), eq(schemaProvider));
  }

  @Test
  public void testTwoFilesAndContinueInSameCommit() throws IOException {
    String commitTimeForWrites = "2";
    String commitTimeForReads = "1";

    Pair<String, List<HoodieRecord>> inserts = writeGcsMetadataRecords(commitTimeForWrites);
    List<CloudObjectMetadata> cloudObjectMetadataList = Arrays.asList(
        new CloudObjectMetadata("data-file-1.json", 1),
        new CloudObjectMetadata("data-file-2.json", 1));
    when(gcsObjectMetadataFetcher.getGcsObjectMetadata(Mockito.any(), Mockito.any(), anyBoolean())).thenReturn(cloudObjectMetadataList);

    List<Row> recs = Arrays.asList(
        new GenericRow(new String[] {"1", "Hello 1"}),
        new GenericRow(new String[] {"2", "Hello 2"}),
        new GenericRow(new String[] {"3", "Hello 3"}),
        new GenericRow(new String[] {"4", "Hello 4"})
    );
    StructType schema = new StructType(new StructField[] {
        DataTypes.createStructField("id", DataTypes.StringType, true),
        DataTypes.createStructField("text", DataTypes.StringType, true)
    });
    Dataset<Row> rows = spark().createDataFrame(recs, schema);

    List<Triple<String, Long, String>> filePathSizeAndCommitTime = new ArrayList<>();
    // Add file paths and sizes to the list
    filePathSizeAndCommitTime.add(Triple.of("path/to/file1.json", 100L, "1"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/file2.json", 150L, "1"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/file3.json", 200L, "1"));

    Dataset<Row> inputDs = generateDataset(filePathSizeAndCommitTime);

    when(gcsObjectDataFetcher.getCloudObjectDataDF(Mockito.any(), eq(cloudObjectMetadataList), Mockito.any(),
        eq(schemaProvider))).thenReturn(Option.of(rows));
    when(queryRunner.run(Mockito.any())).thenReturn(inputDs);

    readAndAssert(READ_UPTO_LATEST_COMMIT, Option.of(commitTimeForReads), 250L, 4, "1#path/to/file2.json");
    readAndAssert(READ_UPTO_LATEST_COMMIT, Option.of("1#path/to/file2.json"), 250L, 4, "1#path/to/file3.json");

    verify(gcsObjectMetadataFetcher, times(2)).getGcsObjectMetadata(Mockito.any(), Mockito.any(),
        anyBoolean());
    verify(gcsObjectDataFetcher, times(2)).getCloudObjectDataDF(Mockito.any(),
        eq(cloudObjectMetadataList), Mockito.any(), eq(schemaProvider));
  }

  @Test
  public void testTwoFilesAndContinueAcrossCommits() throws IOException {
    String commitTimeForWrites = "2";
    String commitTimeForReads = "1";

    Pair<String, List<HoodieRecord>> inserts = writeGcsMetadataRecords(commitTimeForWrites);
    List<CloudObjectMetadata> cloudObjectMetadataList = Arrays.asList(
        new CloudObjectMetadata("data-file-1.json", 1),
        new CloudObjectMetadata("data-file-2.json", 1));
    when(gcsObjectMetadataFetcher.getGcsObjectMetadata(Mockito.any(), Mockito.any(), anyBoolean())).thenReturn(cloudObjectMetadataList);

    List<Row> recs = Arrays.asList(
        new GenericRow(new String[] {"1", "Hello 1"}),
        new GenericRow(new String[] {"2", "Hello 2"}),
        new GenericRow(new String[] {"3", "Hello 3"}),
        new GenericRow(new String[] {"4", "Hello 4"})
    );
    StructType schema = new StructType(new StructField[] {
        DataTypes.createStructField("id", DataTypes.StringType, true),
        DataTypes.createStructField("text", DataTypes.StringType, true)
    });
    Dataset<Row> rows = spark().createDataFrame(recs, schema);

    List<Triple<String, Long, String>> filePathSizeAndCommitTime = new ArrayList<>();
    // Add file paths and sizes to the list
    filePathSizeAndCommitTime.add(Triple.of("path/to/file1.json", 100L, "1"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/file3.json", 200L, "1"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/file2.json", 150L, "1"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/file4.json", 50L, "2"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/file5.json", 150L, "2"));

    Dataset<Row> inputDs = generateDataset(filePathSizeAndCommitTime);

    when(gcsObjectDataFetcher.getCloudObjectDataDF(Mockito.any(), eq(cloudObjectMetadataList), Mockito.any(),
        eq(schemaProvider))).thenReturn(Option.of(rows));
    when(queryRunner.run(Mockito.any())).thenReturn(inputDs);

    readAndAssert(READ_UPTO_LATEST_COMMIT, Option.of(commitTimeForReads), 100L, 4, "1#path/to/file1.json");
    readAndAssert(READ_UPTO_LATEST_COMMIT, Option.of("1#path/to/file1.json"), 100L, 4, "1#path/to/file2.json");
    readAndAssert(READ_UPTO_LATEST_COMMIT, Option.of("1#path/to/file2.json"), 1000L, 4, "2#path/to/file5.json");

    verify(gcsObjectMetadataFetcher, times(3)).getGcsObjectMetadata(Mockito.any(), Mockito.any(),
        anyBoolean());
    verify(gcsObjectDataFetcher, times(3)).getCloudObjectDataDF(Mockito.any(),
        eq(cloudObjectMetadataList), Mockito.any(), eq(schemaProvider));

    schemaProvider = Option.empty();
    when(gcsObjectDataFetcher.getCloudObjectDataDF(Mockito.any(), eq(cloudObjectMetadataList), Mockito.any(),
        eq(schemaProvider))).thenReturn(Option.of(rows));
    readAndAssert(READ_UPTO_LATEST_COMMIT, Option.of(commitTimeForReads), 100L, 4, "1#path/to/file1.json");
  }

  private void readAndAssert(IncrSourceHelper.MissingCheckpointStrategy missingCheckpointStrategy,
                             Option<String> checkpointToPull, long sourceLimit, int expectedCount, String expectedCheckpoint) {
    TypedProperties typedProperties = setProps(missingCheckpointStrategy);

    GcsEventsHoodieIncrSource incrSource = new GcsEventsHoodieIncrSource(typedProperties, jsc(),
        spark(), schemaProvider.orElse(null), gcsObjectMetadataFetcher, gcsObjectDataFetcher, queryRunner);

    Pair<Option<Dataset<Row>>, String> dataAndCheckpoint = incrSource.fetchNextBatch(checkpointToPull, sourceLimit);

    Option<Dataset<Row>> datasetOpt = dataAndCheckpoint.getLeft();
    String nextCheckPoint = dataAndCheckpoint.getRight();

    Assertions.assertNotNull(nextCheckPoint);

    if (expectedCount == 0) {
      assertFalse(datasetOpt.isPresent());
    } else {
      assertEquals(datasetOpt.get().count(), expectedCount);
    }

    Assertions.assertEquals(expectedCheckpoint, nextCheckPoint);
  }

  private HoodieRecord getGcsMetadataRecord(String commitTime, String filename, String bucketName, String generation) {
    String partitionPath = bucketName;

    String id = "id:" + bucketName + "/" + filename + "/" + generation;
    String mediaLink = String.format("https://storage.googleapis.com/download/storage/v1/b/%s/o/%s"
        + "?generation=%s&alt=media", bucketName, filename, generation);
    String selfLink = String.format("https://www.googleapis.com/storage/v1/b/%s/o/%s", bucketName, filename);

    GenericRecord rec = new GenericData.Record(GCS_METADATA_SCHEMA);
    rec.put("_row_key", id);
    rec.put("partition_path", bucketName);
    rec.put("timestamp", Long.parseLong(commitTime));

    rec.put("bucket", bucketName);
    rec.put("contentLanguage", "en");
    rec.put("contentType", "application/octet-stream");
    rec.put("crc32c", "oRB3Aw==");
    rec.put("etag", "CP7EwYCu6/kCEAE=");
    rec.put("generation", generation);
    rec.put("id", id);
    rec.put("kind", "storage#object");
    rec.put("md5Hash", "McsS8FkcDSrB3cGfb18ysA==");
    rec.put("mediaLink", mediaLink);
    rec.put("metageneration", "1");
    rec.put("name", filename);
    rec.put("selfLink", selfLink);
    rec.put("size", "370");
    rec.put("storageClass", "STANDARD");
    rec.put("timeCreated", "2022-08-29T05:52:55.869Z");
    rec.put("timeStorageClassUpdated", "2022-08-29T05:52:55.869Z");
    rec.put("updated", "2022-08-29T05:52:55.869Z");

    HoodieAvroPayload payload = new HoodieAvroPayload(Option.of(rec));
    return new HoodieAvroRecord(new HoodieKey(id, partitionPath), payload);
  }

  private HoodieWriteConfig getWriteConfig() {
    return getConfigBuilder(basePath(), metaClient)
        .withArchivalConfig(HoodieArchivalConfig.newBuilder().archiveCommitsWith(4, 5).build())
            .withCleanConfig(HoodieCleanConfig.newBuilder().retainCommits(1).build())
            .withMetadataConfig(HoodieMetadataConfig.newBuilder()
                    .withMaxNumDeltaCommitsBeforeCompaction(1).build())
            .build();
  }

  private Pair<String, List<HoodieRecord>> writeGcsMetadataRecords(String commitTime) throws IOException {
    HoodieWriteConfig writeConfig = getWriteConfig();
    SparkRDDWriteClient writeClient = getHoodieWriteClient(writeConfig);

    writeClient.startCommitWithTime(commitTime);
    List<HoodieRecord> gcsMetadataRecords = Arrays.asList(
            getGcsMetadataRecord(commitTime, "data-file-1.json", "bucket-1", "1"),
            getGcsMetadataRecord(commitTime, "data-file-2.json", "bucket-1", "1"),
            getGcsMetadataRecord(commitTime, "data-file-3.json", "bucket-1", "1"),
            getGcsMetadataRecord(commitTime, "data-file-4.json", "bucket-1", "1")
    );
    JavaRDD<WriteStatus> result = writeClient.upsert(jsc().parallelize(gcsMetadataRecords, 1), commitTime);

    List<WriteStatus> statuses = result.collect();
    assertNoWriteErrors(statuses);

    return Pair.of(commitTime, gcsMetadataRecords);
  }

  private TypedProperties setProps(IncrSourceHelper.MissingCheckpointStrategy missingCheckpointStrategy) {
    Properties properties = new Properties();
    properties.setProperty("hoodie.deltastreamer.source.hoodieincr.path", basePath());
    properties.setProperty("hoodie.deltastreamer.source.hoodieincr.missing.checkpoint.strategy",
            missingCheckpointStrategy.name());
    properties.setProperty("hoodie.deltastreamer.source.gcsincr.datafile.format", "json");
    return new TypedProperties(properties);
  }

  private HoodieWriteConfig.Builder getConfigBuilder(String basePath, HoodieTableMetaClient metaClient) {
    return HoodieWriteConfig.newBuilder()
            .withPath(basePath)
            .withSchema(GCS_METADATA_SCHEMA.toString())
            .withParallelism(2, 2)
            .withBulkInsertParallelism(2)
            .withFinalizeWriteParallelism(2).withDeleteParallelism(2)
            .withTimelineLayoutVersion(TimelineLayoutVersion.CURR_VERSION)
            .forTable(metaClient.getTableConfig().getTableName());
  }

  private String generateGCSEventMetadata(Long objectSize, String bucketName, String objectKey, String commitTime)
      throws JsonProcessingException {
    Map<String, Object> objectMetadata = new HashMap<>();
    objectMetadata.put("bucket", bucketName);
    objectMetadata.put("name", objectKey);
    objectMetadata.put("size", objectSize);
    objectMetadata.put("_hoodie_commit_time", commitTime);
    return mapper.writeValueAsString(objectMetadata);
  }

  private List<String> getSampleGCSObjectKeys(List<Triple<String, Long, String>> filePathSizeAndCommitTime) {
    return filePathSizeAndCommitTime.stream().map(f -> {
      try {
        return generateGCSEventMetadata(f.getMiddle(), "bucket-1", f.getLeft(), f.getRight());
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
    }).collect(Collectors.toList());
  }

  private Dataset<Row> generateDataset(List<Triple<String, Long, String>> filePathSizeAndCommitTime) {
    JavaRDD<String> testRdd = jsc.parallelize(getSampleGCSObjectKeys(filePathSizeAndCommitTime), 2);
    Dataset<Row> inputDs = spark().read().json(testRdd);
    return inputDs;
  }

}