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

package org.apache.hudi.utilities.sources.helpers;

import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;
import org.apache.hudi.testutils.HoodieSparkClientTestHarness;
import org.apache.hudi.utilities.config.CloudSourceConfig;
import org.apache.hudi.utilities.config.S3EventsHoodieIncrSourceConfig;
import org.apache.hudi.utilities.schema.FilebasedSchemaProvider;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class TestCloudObjectsSelectorCommon extends HoodieSparkClientTestHarness {

  @BeforeEach
  void setUp() {
    initSparkContexts();
  }

  @AfterEach
  void teardown() throws Exception {
    cleanupResources();
  }

  @Test
  void emptyMetadataReturnsEmptyOption() {
    CloudObjectsSelectorCommon cloudObjectsSelectorCommon = new CloudObjectsSelectorCommon(new TypedProperties());
    Option<Dataset<Row>> result = cloudObjectsSelectorCommon.loadAsDataset(sparkSession, Collections.emptyList(), "json", Option.empty(), 1);
    Assertions.assertFalse(result.isPresent());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void filesFromMetadataRead(boolean includeSourcePathField) {
    TypedProperties properties = new TypedProperties();
    if (includeSourcePathField) {
      includeSourcePathFieldProp(properties);
    }
    CloudObjectsSelectorCommon cloudObjectsSelectorCommon = new CloudObjectsSelectorCommon(properties);
    String dataPath = "src/test/resources/data/partitioned/country=US/state=CA/data.json";
    List<CloudObjectMetadata> input = Collections.singletonList(new CloudObjectMetadata(dataPath, 1));
    Option<Dataset<Row>> result = cloudObjectsSelectorCommon.loadAsDataset(sparkSession, input, "json", Option.empty(), 1);

    Assertions.assertTrue(result.isPresent());
    assertRowResult(includeSourcePathField, Collections.singletonList(dataPath), result.get(),
        new Object[]{"some data"});
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void partitionValueAddedToRow(boolean includeSourcePathField) {
    String dataPath = "src/test/resources/data/partitioned/country=US/state=CA/data.json";
    List<CloudObjectMetadata> input = Collections.singletonList(new CloudObjectMetadata(dataPath, 1));

    TypedProperties properties = new TypedProperties();
    properties.put("hoodie.streamer.source.cloud.data.partition.fields.from.path", "country,state");
    properties.put("hoodie.deltastreamer.source.cloud.data.partition.fields.from.path", "country,state");
    if (includeSourcePathField) {
      includeSourcePathFieldProp(properties);
    }
    CloudObjectsSelectorCommon cloudObjectsSelectorCommon = new CloudObjectsSelectorCommon(properties);
    Option<Dataset<Row>> result = cloudObjectsSelectorCommon.loadAsDataset(sparkSession, input, "json", Option.empty(), 1);

    Assertions.assertTrue(result.isPresent());
    assertRowResult(includeSourcePathField, Collections.singletonList(dataPath), result.get(),
        new Object[]{"some data", "US", "CA"});
  }

  @Test
  void loadDatasetWithSchema() {
    TypedProperties props = new TypedProperties();
    TestCloudObjectsSelectorCommon.class.getClassLoader().getResource("schema/sample_data_schema.avsc");
    String schemaFilePath = TestCloudObjectsSelectorCommon.class.getClassLoader().getResource("schema/sample_data_schema.avsc").getPath();
    props.put("hoodie.streamer.schemaprovider.source.schema.file", schemaFilePath);
    props.put("hoodie.streamer.schema.provider.class.name", FilebasedSchemaProvider.class.getName());
    props.put("hoodie.streamer.source.cloud.data.partition.fields.from.path", "country,state");
    CloudObjectsSelectorCommon cloudObjectsSelectorCommon = new CloudObjectsSelectorCommon(props);
    List<CloudObjectMetadata> input = Collections.singletonList(new CloudObjectMetadata("src/test/resources/data/partitioned/country=US/state=CA/data.json", 1));
    Option<Dataset<Row>> result = cloudObjectsSelectorCommon.loadAsDataset(sparkSession, input, "json", Option.of(new FilebasedSchemaProvider(props, jsc)), 1);
    Assertions.assertTrue(result.isPresent());
    Assertions.assertEquals(1, result.get().count());
    Row expected = RowFactory.create("some data", "US", "CA");
    Assertions.assertEquals(Collections.singletonList(expected), result.get().collectAsList());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void loadDatasetWithSchemaAndAliasFields(boolean includeSourcePathField) {
    TypedProperties props = new TypedProperties();
    TestCloudObjectsSelectorCommon.class.getClassLoader().getResource("schema/sample_data_schema.avsc");
    String schemaFilePath = TestCloudObjectsSelectorCommon.class.getClassLoader().getResource("schema/sample_data_schema.avsc").getPath();
    props.put("hoodie.deltastreamer.schemaprovider.source.schema.file", schemaFilePath);
    props.put("hoodie.deltastreamer.schema.provider.class.name", FilebasedSchemaProvider.class.getName());
    props.put("hoodie.deltastreamer.source.cloud.data.partition.fields.from.path", "country,state");
    props.put("hoodie.streamer.source.cloud.data.reader.coalesce.aliases", "true");
    if (includeSourcePathField) {
      includeSourcePathFieldProp(props);
    }
    CloudObjectsSelectorCommon cloudObjectsSelectorCommon = new CloudObjectsSelectorCommon(props);
    String dataPath = "src/test/resources/data/partitioned/country=US/state=TX/old_data.json";
    List<CloudObjectMetadata> input = Collections.singletonList(new CloudObjectMetadata(dataPath, 1));
    Option<Dataset<Row>> result = cloudObjectsSelectorCommon.loadAsDataset(sparkSession, input, "json", Option.of(new FilebasedSchemaProvider(props, jsc)), 1);

    Assertions.assertTrue(result.isPresent());
    assertRowResult(includeSourcePathField, Collections.singletonList(dataPath), result.get(),
        new Object[]{"some data", "US", "TX"});
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void loadDatasetWithSchemaAndRepartition(boolean includeSourcePathField) {
    TypedProperties props = new TypedProperties();
    String schemaFilePath = TestCloudObjectsSelectorCommon.class.getClassLoader().getResource("schema/sample_data_schema.avsc").getPath();
    props.put("hoodie.streamer.schemaprovider.source.schema.file", schemaFilePath);
    props.put("hoodie.streamer.schema.provider.class.name", FilebasedSchemaProvider.class.getName());
    props.put("hoodie.streamer.source.cloud.data.partition.fields.from.path", "country,state");
    // Setting this config so that dataset repartition happens inside `loadAsDataset`
    props.put("hoodie.streamer.source.cloud.data.partition.max.size", "1");

    if (includeSourcePathField) {
      includeSourcePathFieldProp(props);
    }

    String dataPath1 = "src/test/resources/data/partitioned/country=US/state=CA/data.json";
    String dataPath2 = "src/test/resources/data/partitioned/country=US/state=TX/data.json";
    String dataPath3 = "src/test/resources/data/partitioned/country=IND/state=TS/data.json";

    List<CloudObjectMetadata> input = Arrays.asList(
        new CloudObjectMetadata(dataPath1, 1000),
        new CloudObjectMetadata(dataPath2, 1000),
        new CloudObjectMetadata(dataPath3, 1000));

    CloudObjectsSelectorCommon cloudObjectsSelectorCommon = new CloudObjectsSelectorCommon(props);
    Option<Dataset<Row>> result = cloudObjectsSelectorCommon.loadAsDataset(sparkSession, input, "json", Option.of(new FilebasedSchemaProvider(props, jsc)), 30);

    Assertions.assertTrue(result.isPresent());
    assertRowResult(
        includeSourcePathField,
        Arrays.asList(dataPath1, dataPath2, dataPath3),
        result.get(),
        new Object[]{"some data", "US", "CA"},
        new Object[]{"some data", "US", "TX"},
        new Object[]{"some data", "IND", "TS"});
  }

  @Test
  void loadDatasetWithSchemaAndCoalesceAliases() {
    TypedProperties props = new TypedProperties();
    TestCloudObjectsSelectorCommon.class.getClassLoader().getResource("schema/sample_data_schema.avsc");
    String schemaFilePath = TestCloudObjectsSelectorCommon.class.getClassLoader().getResource("schema/sample_data_schema.avsc").getPath();
    props.put("hoodie.deltastreamer.schemaprovider.source.schema.file", schemaFilePath);
    props.put("hoodie.deltastreamer.schema.provider.class.name", FilebasedSchemaProvider.class.getName());
    props.put("hoodie.deltastreamer.source.cloud.data.partition.fields.from.path", "country,state");
    // Setting this config so that dataset repartition happens inside `loadAsDataset`
    props.put("hoodie.streamer.source.cloud.data.partition.max.size", "1");
    props.put("hoodie.streamer.source.cloud.data.reader.coalesce.aliases", "true");
    List<CloudObjectMetadata> input = Arrays.asList(
        new CloudObjectMetadata("src/test/resources/data/partitioned/country=US/state=CA/data.json", 1000),
        new CloudObjectMetadata("src/test/resources/data/partitioned/country=US/state=TX/old_data.json", 1000),
        new CloudObjectMetadata("src/test/resources/data/partitioned/country=IND/state=TS/data.json", 1000)
    );
    CloudObjectsSelectorCommon cloudObjectsSelectorCommon = new CloudObjectsSelectorCommon(props);
    Option<Dataset<Row>> result = cloudObjectsSelectorCommon.loadAsDataset(sparkSession, input, "json", Option.of(new FilebasedSchemaProvider(props, jsc)), 30);
    Assertions.assertTrue(result.isPresent());
    List<Row> expected = Arrays.asList(RowFactory.create("some data", "US", "CA"), RowFactory.create("some data", "US", "TX"), RowFactory.create("some data", "IND", "TS"));
    List<Row> actual = result.get().collectAsList();
    Assertions.assertEquals(new HashSet<>(expected), new HashSet<>(actual));
  }

  @Test
  void loadDatasetWithNestedSchemaAndCoalesceAliases() throws IOException {
    TypedProperties props = new TypedProperties();
    TestCloudObjectsSelectorCommon.class.getClassLoader().getResource("schema/nested_data_schema.avsc");
    String schemaFilePath = TestCloudObjectsSelectorCommon.class.getClassLoader().getResource("schema/nested_data_schema.avsc").getPath();
    props.put("hoodie.deltastreamer.schemaprovider.source.schema.file", schemaFilePath);
    props.put("hoodie.deltastreamer.schema.provider.class.name", FilebasedSchemaProvider.class.getName());
    // Setting this config so that dataset repartition happens inside `loadAsDataset`
    props.put("hoodie.streamer.source.cloud.data.partition.max.size", "1");
    props.put("hoodie.streamer.source.cloud.data.reader.coalesce.aliases", "true");
    List<CloudObjectMetadata> input = Arrays.asList(
        new CloudObjectMetadata("src/test/resources/data/nested_data_1.json", 1000),
        new CloudObjectMetadata("src/test/resources/data/nested_data_2.json", 1000),
        new CloudObjectMetadata("src/test/resources/data/nested_data_3.json", 1000)
    );
    CloudObjectsSelectorCommon cloudObjectsSelectorCommon = new CloudObjectsSelectorCommon(props);
    Option<Dataset<Row>> result = cloudObjectsSelectorCommon.loadAsDataset(sparkSession, input, "json", Option.of(new FilebasedSchemaProvider(props, jsc)), 30);
    Assertions.assertTrue(result.isPresent());
    Row address1 = RowFactory.create("123 Main St", "Springfield", "12345", RowFactory.create("India", "IN"));
    Row person1 = RowFactory.create("John", "Doe", RowFactory.create(1990, 5, 15), address1);
    Row address2 = RowFactory.create("456 Elm St", "Shelbyville", "67890", RowFactory.create("Spain", "SPN"));
    Row person2 = RowFactory.create("Jane", "Smith", RowFactory.create(1992, 9, 2), address2);
    Row address3 = RowFactory.create("789 Maple Ave", "Paris", "98765", RowFactory.create("France", "FRA"));
    Row person3 = RowFactory.create("John", "James", RowFactory.create(1985, 6, 15), address3);
    List<Row> expected = Arrays.asList(person1, person2, person3);
    List<Row> actual = result.get().collectAsList();
    Assertions.assertEquals(new HashSet<>(expected), new HashSet<>(actual));
    Schema schema = new Schema.Parser().parse(new File(schemaFilePath));
    StructType expectedSchema = AvroConversionUtils.convertAvroSchemaToStructType(schema);
    // assert final output schema matches with the source schema
    Assertions.assertEquals(expectedSchema, result.get().schema(), "output dataset schema should match source schema");
  }

  @Test
  void partitionKeyNotPresentInPath() {
    List<CloudObjectMetadata> input = Collections.singletonList(new CloudObjectMetadata("src/test/resources/data/partitioned/country=US/state=CA/data.json", 1));
    TypedProperties properties = new TypedProperties();
    properties.put("hoodie.deltastreamer.source.cloud.data.reader.comma.separated.path.format", "false");
    properties.put("hoodie.deltastreamer.source.cloud.data.partition.fields.from.path", "unknown");
    CloudObjectsSelectorCommon cloudObjectsSelectorCommon = new CloudObjectsSelectorCommon(properties);
    Option<Dataset<Row>> result = cloudObjectsSelectorCommon.loadAsDataset(sparkSession, input, "json", Option.empty(), 1);
    Assertions.assertTrue(result.isPresent());
    Assertions.assertEquals(1, result.get().count());
    Row expected = RowFactory.create("some data", null);
    Assertions.assertEquals(Collections.singletonList(expected), result.get().collectAsList());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testWithSourcePathColumnAlreadyExistsInDataset(boolean includeSourcePathField) {
    String dataPath = "src/test/resources/data/data_with_sourcePath_column.json";
    List<CloudObjectMetadata> input = Collections.singletonList(new CloudObjectMetadata(dataPath, 1));
    TypedProperties properties = new TypedProperties();
    if (includeSourcePathField) {
      includeSourcePathFieldProp(properties);
    }
    CloudObjectsSelectorCommon cloudObjectsSelectorCommon = new CloudObjectsSelectorCommon(properties);

    if (includeSourcePathField) {
      Exception exception = Assertions.assertThrows(Exception.class,
          () -> cloudObjectsSelectorCommon.loadAsDataset(sparkSession, input, "json", Option.empty(), 1));
      Assertions.assertTrue(exception.getMessage().contains("Column 'sourcePath' already exists in the dataset"));
    } else {
      Option<Dataset<Row>> result = cloudObjectsSelectorCommon.loadAsDataset(sparkSession, input, "json", Option.empty(), 1);
      Assertions.assertTrue(result.isPresent());
      Assertions.assertEquals(1, result.get().count());
      Assertions.assertEquals(Collections.singletonList(RowFactory.create("some data", "existing/path")), result.get().collectAsList());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testWithNestedSourcePathColumnExistsInDataset(boolean includeSourcePathField) {
    String dataPath = "src/test/resources/data/data_with_nested_sourcePath_column.json";
    List<CloudObjectMetadata> input = Collections.singletonList(new CloudObjectMetadata(dataPath, 1));
    TypedProperties properties = new TypedProperties();
    if (includeSourcePathField) {
      includeSourcePathFieldProp(properties);
    }
    CloudObjectsSelectorCommon cloudObjectsSelectorCommon = new CloudObjectsSelectorCommon(properties);

    Option<Dataset<Row>> result = cloudObjectsSelectorCommon.loadAsDataset(sparkSession, input, "json", Option.empty(), 1);
    Assertions.assertTrue(result.isPresent());
    Assertions.assertEquals(1, result.get().count());

    Row nestedMetadata = RowFactory.create("nested/path");
    Row expectedRow;
    if (includeSourcePathField) {
      Assertions.assertTrue(Arrays.asList(result.get().schema().fieldNames()).contains("sourcePath"));
      String expectedSourcePath = "file://" + new Path(new File(dataPath).getAbsolutePath()).toUri().getPath();
      expectedRow = RowFactory.create(expectedSourcePath, "some data", nestedMetadata);
    } else {
      Assertions.assertFalse(Arrays.asList(result.get().schema().fieldNames()).contains("sourcePath"));
      expectedRow = RowFactory.create("some data", nestedMetadata);
    }
    Assertions.assertEquals(Collections.singletonList(expectedRow), result.get().collectAsList());
  }

  @Test
  void testGetObjectMetadataRepartitions() {
    // Create a DataFrame simulating S3 event metadata with the nested column structure
    // that getObjectMetadata expects: s3.bucket.name, s3.object.key, s3.object.size
    List<String> jsonRecords = Arrays.asList(
        "{\"s3\":{\"bucket\":{\"name\":\"test-bucket\"},\"object\":{\"key\":\"path/file1.json\",\"size\":100}}}",
        "{\"s3\":{\"bucket\":{\"name\":\"test-bucket\"},\"object\":{\"key\":\"path/file2.json\",\"size\":200}}}",
        "{\"s3\":{\"bucket\":{\"name\":\"test-bucket\"},\"object\":{\"key\":\"path/file3.json\",\"size\":300}}}",
        // duplicate to verify distinct() works
        "{\"s3\":{\"bucket\":{\"name\":\"test-bucket\"},\"object\":{\"key\":\"path/file1.json\",\"size\":100}}}"
    );
    Dataset<Row> cloudObjectMetadataDF = sparkSession.read().json(
        sparkSession.createDataset(jsonRecords, Encoders.STRING()));

    TypedProperties props = new TypedProperties();
    props.put(CloudSourceConfig.ENABLE_EXISTS_CHECK.key(), "false");
    props.put(CloudSourceConfig.EXISTS_CHECK_PARALLELISM.key(), "2");
    // s3FS prefix required for S3 type
    props.put(S3EventsHoodieIncrSourceConfig.S3_FS_PREFIX.key(), "s3");

    List<CloudObjectMetadata> result = CloudObjectsSelectorCommon.getObjectMetadata(
        CloudObjectsSelectorCommon.Type.S3, jsc, cloudObjectMetadataDF, false, props);

    // Should return 3 distinct files (duplicate removed)
    Assertions.assertEquals(3, result.size());
    Set<String> paths = result.stream().map(CloudObjectMetadata::getPath).collect(Collectors.toSet());
    Assertions.assertTrue(paths.contains("s3://test-bucket/path/file1.json"));
    Assertions.assertTrue(paths.contains("s3://test-bucket/path/file2.json"));
    Assertions.assertTrue(paths.contains("s3://test-bucket/path/file3.json"));

    // Also validate sizes
    Map<String, Long> pathToSize = result.stream()
        .collect(Collectors.toMap(CloudObjectMetadata::getPath, CloudObjectMetadata::getSize));
    Assertions.assertEquals(100L, pathToSize.get("s3://test-bucket/path/file1.json"));
    Assertions.assertEquals(200L, pathToSize.get("s3://test-bucket/path/file2.json"));
    Assertions.assertEquals(300L, pathToSize.get("s3://test-bucket/path/file3.json"));
  }

  @Test
  void testGetObjectMetadataWithExistsCheck() {
    // Test the full getObjectMetadata path with checkIfExists=true using real local files.
    // Use file:// as the S3 FS prefix so URLs resolve to local filesystem.
    String existingFile = new File("src/test/resources/data/partitioned/country=US/state=CA/data.json")
        .toURI().getPath().substring(1);
    String nonExistentFile = "tmp/non_existent_file_" + System.nanoTime() + ".json";

    List<String> jsonRecords = Arrays.asList(
        String.format("{\"s3\":{\"bucket\":{\"name\":\"\"},\"object\":{\"key\":\"%s\",\"size\":100}}}", existingFile),
        String.format("{\"s3\":{\"bucket\":{\"name\":\"\"},\"object\":{\"key\":\"%s\",\"size\":200}}}", nonExistentFile),
        // duplicate of existing file
        String.format("{\"s3\":{\"bucket\":{\"name\":\"\"},\"object\":{\"key\":\"%s\",\"size\":100}}}", existingFile)
    );
    Dataset<Row> cloudObjectMetadataDF = sparkSession.read().json(
        sparkSession.createDataset(jsonRecords, Encoders.STRING()));

    TypedProperties props = new TypedProperties();
    props.put(CloudSourceConfig.EXISTS_CHECK_PARALLELISM.key(), "4");
    props.put(S3EventsHoodieIncrSourceConfig.S3_FS_PREFIX.key(), "file");

    List<CloudObjectMetadata> result = CloudObjectsSelectorCommon.getObjectMetadata(
        CloudObjectsSelectorCommon.Type.S3, jsc, cloudObjectMetadataDF, true, props);

    // Only the existing file should be returned (non-existent filtered, duplicate deduped)
    Assertions.assertEquals(1, result.size());
    Assertions.assertEquals("file:///" + existingFile, result.get(0).getPath());
    Assertions.assertEquals(100L, result.get(0).getSize());
  }

  @Test
  void testParallelExistsCheck() throws Exception {
    StorageConfiguration<Configuration> storageConf = new HadoopStorageConfiguration(jsc.hadoopConfiguration());

    // Use real test resource files (strip leading "/" so URL becomes file:///abs/path)
    String existingFile1 = new File("src/test/resources/data/partitioned/country=US/state=CA/data.json")
        .toURI().getPath().substring(1);
    String existingFile2 = new File("src/test/resources/data/partitioned/country=US/state=TX/data.json")
        .toURI().getPath().substring(1);
    String nonExistentFile = "tmp/non_existent_file_" + System.nanoTime() + ".json";

    // Row schema: [bucket, key, size] — URL = prefix + bucket + "/" + key
    List<Row> rows = Arrays.asList(
        RowFactory.create("", existingFile1, 100L),
        RowFactory.create("", existingFile2, 200L),
        RowFactory.create("", nonExistentFile, 300L)
    );

    // Parallel path: checkIfExists=true, parallelism=8
    Iterator<CloudObjectMetadata> iter = CloudObjectsSelectorCommon
        .getCloudObjectMetadataPerPartition("file://", storageConf, true, 8)
        .call(rows.iterator());
    List<CloudObjectMetadata> result = new ArrayList<>();
    iter.forEachRemaining(result::add);

    // Non-existent file should be filtered out
    Assertions.assertEquals(2, result.size());
    Map<String, Long> pathToSize = result.stream()
        .collect(Collectors.toMap(CloudObjectMetadata::getPath, CloudObjectMetadata::getSize));
    Assertions.assertEquals(100L, pathToSize.get("file:///" + existingFile1));
    Assertions.assertEquals(200L, pathToSize.get("file:///" + existingFile2));
    Assertions.assertFalse(pathToSize.containsKey("file:///" + nonExistentFile));
  }

  @Test
  void testSequentialExistsCheckFallback() throws Exception {
    StorageConfiguration<Configuration> storageConf = new HadoopStorageConfiguration(jsc.hadoopConfiguration());

    String existingFile = new File("src/test/resources/data/partitioned/country=US/state=CA/data.json")
        .toURI().getPath().substring(1);
    String nonExistentFile = "tmp/non_existent_file_" + System.nanoTime() + ".json";

    List<Row> rows = Arrays.asList(
        RowFactory.create("", existingFile, 100L),
        RowFactory.create("", nonExistentFile, 200L)
    );

    // Sequential fallback: checkIfExists=true, parallelism=1
    Iterator<CloudObjectMetadata> iter = CloudObjectsSelectorCommon
        .getCloudObjectMetadataPerPartition("file://", storageConf, true, 1)
        .call(rows.iterator());
    List<CloudObjectMetadata> result = new ArrayList<>();
    iter.forEachRemaining(result::add);

    Assertions.assertEquals(1, result.size());
    Assertions.assertEquals("file:///" + existingFile, result.get(0).getPath());
    Assertions.assertEquals(100L, result.get(0).getSize());
  }

  /**
   * Asserts that a Dataset contains expected rows, additionally checking for sourcePath if present.
   */
  private void assertRowResult(
      boolean includeSourcePathField,
      List<String> dataPaths,
      Dataset<Row> actualResult,
      Object[]... rowContents) {
    Assertions.assertEquals(rowContents.length, actualResult.count());

    List<Row> expected = new ArrayList<>();
    if (includeSourcePathField) {
      Assertions.assertTrue(Arrays.asList(actualResult.schema().fieldNames()).contains("sourcePath"));
      for (int i = 0; i < dataPaths.size(); i++) {
        // Spark adds a leading slash to the path when using local file system
        String expectedPath = "file://" + new Path(new File(dataPaths.get(i)).getAbsolutePath()).toUri().getPath();
        List<Object> values = new ArrayList<>();
        values.add(expectedPath);
        values.addAll(Arrays.asList(rowContents[i]));
        expected.add(RowFactory.create(values.toArray()));
      }
    } else {
      Assertions.assertFalse(Arrays.asList(actualResult.schema().fieldNames()).contains("sourcePath"));
      for (Object[] row : rowContents) {
        expected.add(RowFactory.create(row));
      }
    }

    List<Row> actual = actualResult.collectAsList();
    Assertions.assertEquals(new HashSet<>(expected), new HashSet<>(actual));
  }

  private void includeSourcePathFieldProp(TypedProperties properties) {
    properties.put("hoodie.streamer.source.cloud.data.include.source.path.field", "true");
  }
}
