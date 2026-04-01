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

package org.apache.hudi.client;

import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.FileFormatUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.io.storage.HoodieIOFactory;
import org.apache.hudi.keygen.KeyGenUtils;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;

import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.getCommitTimeAtUTC;
import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for ComplexKeyGenerator auto-deduction of encoding format.
 */
public class TestComplexKeyGenAutoDeduction extends SparkClientFunctionalTestHarness {

  /**
   * Test auto-deduction with two commits where first commit uses specified encoding
   * and second commit auto-deduces and maintains the same encoding.
   */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testAutoDeductionWithTwoCommits(boolean useNewEncoding) throws IOException {
    String recordKeyField = "_row_key";
    String partitionPathField = "partition_path";

    // Setup table properties with ComplexKeyGenerator and single record key field
    Properties tableProps = new Properties();
    tableProps.put(HoodieTableConfig.KEY_GENERATOR_CLASS_NAME.key(), "org.apache.hudi.keygen.ComplexKeyGenerator");
    tableProps.put(HoodieTableConfig.RECORDKEY_FIELDS.key(), recordKeyField);
    tableProps.put(HoodieTableConfig.PARTITION_FIELDS.key(), partitionPathField);
    tableProps.put(HoodieTableConfig.VERSION.key(), "6");

    HoodieTableMetaClient metaClient = getHoodieMetaClient(
        storageConf(), URI.create(basePath()).getPath(), HoodieTableType.COPY_ON_WRITE, tableProps);

    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(0xDEED);

    // First commit: Disable auto-deduction, explicitly set encoding format
    String instant1 = getCommitTimeAtUTC(1);
    HoodieWriteConfig writeConfig1 = getConfigBuilder(true)
        .withPath(metaClient.getBasePath())
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
        .withProps(getKeyGenProps(recordKeyField, partitionPathField, useNewEncoding, false))
        .build();

    try (SparkRDDWriteClient writeClient1 = getHoodieWriteClient(writeConfig1)) {
      writeClient1.startCommitWithTime(instant1);
      List<HoodieRecord> records1 = dataGen.generateInserts(instant1, 10);
      JavaRDD<HoodieRecord> writeRecords1 = jsc().parallelize(records1, 2);
      List<WriteStatus> writeStatuses1 = writeClient1.insert(writeRecords1, instant1).collect();
      assertNoWriteErrors(writeStatuses1);
    }

    // Verify first commit used the specified encoding
    verifyRecordKeyEncoding(metaClient, recordKeyField, useNewEncoding);

    // Verify no aux file exists yet
    StoragePath auxFilePath = KeyGenUtils.getComplexKeyEncodingFilePath(metaClient.getBasePath().toString());
    HoodieStorage storage = metaClient.getStorage();
    assertTrue(!storage.exists(auxFilePath), "Aux file should not exist after first commit");

    // Second commit: Enable auto-deduction
    String instant2 = getCommitTimeAtUTC(2);
    HoodieWriteConfig writeConfig2 = getConfigBuilder(true)
        .withPath(metaClient.getBasePath())
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
        .withProps(getKeyGenPropsWithAutoDeduction(recordKeyField, partitionPathField))
        .build();

    // Reload metaClient to pick up new timeline
    metaClient = HoodieTableMetaClient.reload(metaClient);

    try (SparkRDDWriteClient writeClient2 = getHoodieWriteClient(writeConfig2)) {
      writeClient2.startCommitWithTime(instant2);
      List<HoodieRecord> records2 = dataGen.generateInserts(instant2, 10);
      JavaRDD<HoodieRecord> writeRecords2 = jsc().parallelize(records2, 2);
      List<WriteStatus> writeStatuses2 = writeClient2.insert(writeRecords2, instant2).collect();
      assertNoWriteErrors(writeStatuses2);
    }

    // Verify aux file was created after second commit
    assertTrue(storage.exists(auxFilePath), "Aux file should exist after second commit with auto-deduction");

    // Verify cached encoding matches first commit's encoding
    Option<Boolean> cachedEncoding = KeyGenUtils.readComplexKeyEncodingFromAuxFile(storage, metaClient.getBasePath().toString());
    assertTrue(cachedEncoding.isPresent(), "Cached encoding should be present");
    assertEquals(useNewEncoding, cachedEncoding.get(), "Cached encoding should match first commit's encoding");

    // Reload metaClient
    metaClient = HoodieTableMetaClient.reload(metaClient);

    // Verify second commit used the same encoding as first commit
    verifyRecordKeyEncoding(metaClient, recordKeyField, useNewEncoding);
  }

  private Properties getKeyGenProps(String recordKeyField, String partitionPathField,
                                     boolean useNewEncoding, boolean autoDeduction) {
    Properties props = new Properties();
    props.put(HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key(), "org.apache.hudi.keygen.ComplexKeyGenerator");
    props.put(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), recordKeyField);
    props.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), partitionPathField);
    props.put(HoodieWriteConfig.COMPLEX_KEYGEN_NEW_ENCODING.key(), String.valueOf(useNewEncoding));
    props.put(HoodieWriteConfig.COMPLEX_KEYGEN_AUTO_DEDUCE_ENCODING.key(), String.valueOf(autoDeduction));
    // Disable validation since we're explicitly setting encoding
    props.put(HoodieWriteConfig.ENABLE_COMPLEX_KEYGEN_VALIDATION.key(), "false");
    return props;
  }

  private Properties getKeyGenPropsWithAutoDeduction(String recordKeyField, String partitionPathField) {
    Properties props = new Properties();
    props.put(HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key(), "org.apache.hudi.keygen.ComplexKeyGenerator");
    props.put(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), recordKeyField);
    props.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), partitionPathField);
    props.put(HoodieWriteConfig.COMPLEX_KEYGEN_AUTO_DEDUCE_ENCODING.key(), "true");
    return props;
  }

  /**
   * Test that specifically verifies older encoding (field:value format) is correctly
   * detected and maintained across commits.
   */
  @org.junit.jupiter.api.Test
  public void testOlderEncodingAutoDeduction() throws IOException {
    String recordKeyField = "_row_key";
    String partitionPathField = "partition_path";

    // Setup table properties with ComplexKeyGenerator and single record key field
    Properties tableProps = new Properties();
    tableProps.put(HoodieTableConfig.KEY_GENERATOR_CLASS_NAME.key(), "org.apache.hudi.keygen.ComplexKeyGenerator");
    tableProps.put(HoodieTableConfig.RECORDKEY_FIELDS.key(), recordKeyField);
    tableProps.put(HoodieTableConfig.PARTITION_FIELDS.key(), partitionPathField);
    tableProps.put(HoodieTableConfig.VERSION.key(), "6");

    HoodieTableMetaClient metaClient = getHoodieMetaClient(
        storageConf(), URI.create(basePath()).getPath(), HoodieTableType.COPY_ON_WRITE, tableProps);

    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(0xDEED);

    // First commit: Use older encoding (useNewEncoding = false)
    String instant1 = getCommitTimeAtUTC(1);
    HoodieWriteConfig writeConfig1 = getConfigBuilder(true)
        .withPath(metaClient.getBasePath())
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
        .withProps(getKeyGenProps(recordKeyField, partitionPathField, false, false))
        .build();

    try (SparkRDDWriteClient writeClient1 = getHoodieWriteClient(writeConfig1)) {
      writeClient1.startCommitWithTime(instant1);
      List<HoodieRecord> records1 = dataGen.generateInserts(instant1, 10);
      JavaRDD<HoodieRecord> writeRecords1 = jsc().parallelize(records1, 2);
      List<WriteStatus> writeStatuses1 = writeClient1.insert(writeRecords1, instant1).collect();
      assertNoWriteErrors(writeStatuses1);
    }

    // Verify first commit used older encoding (field:value format)
    HoodieStorage storage = metaClient.getStorage();
    FileFormatUtils fileFormatUtils = HoodieIOFactory.getIOFactory(storage)
        .getFileFormatUtils(HoodieFileFormat.PARQUET);

    List<StoragePath> parquetFiles = storage.globEntries(new StoragePath(metaClient.getBasePath(), "*/*.parquet"))
        .collect(Collectors.toList());
    assertTrue(!parquetFiles.isEmpty(), "Should have at least one parquet file");

    StoragePath firstCommitFile = parquetFiles.get(0);
    try (var keyIterator = fileFormatUtils.getHoodieKeyIterator(storage, firstCommitFile)) {
      assertTrue(keyIterator.hasNext(), "Should have at least one record");
      HoodieKey hoodieKey = keyIterator.next();
      String hoodieRecordKey = hoodieKey.getRecordKey();

      // Verify older encoding format: field:value
      String expectedPrefix = recordKeyField + KeyGenUtils.DEFAULT_COLUMN_VALUE_SEPARATOR;
      assertTrue(hoodieRecordKey.startsWith(expectedPrefix),
          String.format("First commit should use older encoding (field:value). hoodieRecordKey=%s", hoodieRecordKey));
    }

    // Second commit: Enable auto-deduction
    String instant2 = getCommitTimeAtUTC(2);
    HoodieWriteConfig writeConfig2 = getConfigBuilder(true)
        .withPath(metaClient.getBasePath())
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
        .withProps(getKeyGenPropsWithAutoDeduction(recordKeyField, partitionPathField))
        .build();

    metaClient = HoodieTableMetaClient.reload(metaClient);

    try (SparkRDDWriteClient writeClient2 = getHoodieWriteClient(writeConfig2)) {
      writeClient2.startCommitWithTime(instant2);
      List<HoodieRecord> records2 = dataGen.generateInserts(instant2, 10);
      JavaRDD<HoodieRecord> writeRecords2 = jsc().parallelize(records2, 2);
      List<WriteStatus> writeStatuses2 = writeClient2.insert(writeRecords2, instant2).collect();
      assertNoWriteErrors(writeStatuses2);
    }

    // Verify aux file was created with older encoding (useNewEncoding=false)
    StoragePath auxFilePath = KeyGenUtils.getComplexKeyEncodingFilePath(metaClient.getBasePath().toString());
    assertTrue(storage.exists(auxFilePath), "Aux file should exist after second commit");

    Option<Boolean> cachedEncoding = KeyGenUtils.readComplexKeyEncodingFromAuxFile(storage, metaClient.getBasePath().toString());
    assertTrue(cachedEncoding.isPresent(), "Cached encoding should be present");
    assertEquals(false, cachedEncoding.get(), "Cached encoding should be false (older encoding)");

    // Verify second commit also uses older encoding
    metaClient = HoodieTableMetaClient.reload(metaClient);
    List<StoragePath> allParquetFiles = storage.globEntries(new StoragePath(metaClient.getBasePath(), "*/*.parquet"))
        .collect(Collectors.toList());

    // Check a file from the second commit
    StoragePath secondCommitFile = allParquetFiles.stream()
        .filter(f -> f.toString().contains(instant2))
        .findFirst()
        .orElseThrow(() -> new AssertionError("Should have at least one parquet file from second commit"));

    try (var keyIterator = fileFormatUtils.getHoodieKeyIterator(storage, secondCommitFile)) {
      assertTrue(keyIterator.hasNext(), "Should have at least one record");
      HoodieKey hoodieKey = keyIterator.next();
      String hoodieRecordKey = hoodieKey.getRecordKey();

      // Verify second commit also uses older encoding format: field:value
      String expectedPrefix = recordKeyField + KeyGenUtils.DEFAULT_COLUMN_VALUE_SEPARATOR;
      assertTrue(hoodieRecordKey.startsWith(expectedPrefix),
          String.format("Second commit should use older encoding (field:value) after auto-deduction. hoodieRecordKey=%s", hoodieRecordKey));
    }
  }

  private void verifyRecordKeyEncoding(HoodieTableMetaClient metaClient, String recordKeyFieldName, boolean useNewEncoding) throws IOException {
    HoodieStorage storage = metaClient.getStorage();
    FileFormatUtils fileFormatUtils = HoodieIOFactory.getIOFactory(storage)
        .getFileFormatUtils(HoodieFileFormat.PARQUET);

    // Get all parquet files
    List<StoragePath> parquetFiles = storage.globEntries(new StoragePath(metaClient.getBasePath(), "*/*.parquet"))
        .collect(Collectors.toList());

    assertTrue(!parquetFiles.isEmpty(), "Should have at least one parquet file");

    // Check the first parquet file
    StoragePath parquetFile = parquetFiles.get(0);
    try (var keyIterator = fileFormatUtils.getHoodieKeyIterator(storage, parquetFile)) {
      assertTrue(keyIterator.hasNext(), "Should have at least one record");
      HoodieKey hoodieKey = keyIterator.next();
      String hoodieRecordKey = hoodieKey.getRecordKey();

      String expectedPrefix = recordKeyFieldName + KeyGenUtils.DEFAULT_COLUMN_VALUE_SEPARATOR;
      boolean actualUsesNewEncoding = !hoodieRecordKey.startsWith(expectedPrefix);

      assertEquals(useNewEncoding, actualUsesNewEncoding,
          String.format("Record key encoding mismatch. Expected useNewEncoding=%s, hoodieRecordKey=%s",
              useNewEncoding, hoodieRecordKey));
    }
  }
}
