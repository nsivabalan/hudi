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

package org.apache.hudi.keygen;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.HoodieAvroWriteSupport;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.bloom.BloomFilterFactory;
import org.apache.hudi.common.bloom.BloomFilterTypeCode;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieKeyException;
import org.apache.hudi.keygen.constant.KeyGeneratorType;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.apache.hudi.common.table.HoodieTableConfig.KEY_GENERATOR_CLASS_NAME;
import static org.apache.hudi.common.table.HoodieTableConfig.RECORDKEY_FIELDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestKeyGenUtils {

  @Test
  public void testInferKeyGeneratorType() {
    assertEquals(
        KeyGeneratorType.SIMPLE,
        KeyGenUtils.inferKeyGeneratorType(Option.of("col1"), "partition1"));
    assertEquals(
        KeyGeneratorType.COMPLEX,
        KeyGenUtils.inferKeyGeneratorType(Option.of("col1"), "partition1,partition2"));
    assertEquals(
        KeyGeneratorType.COMPLEX,
        KeyGenUtils.inferKeyGeneratorType(Option.of("col1,col2"), "partition1"));
    assertEquals(
        KeyGeneratorType.COMPLEX,
        KeyGenUtils.inferKeyGeneratorType(Option.of("col1,col2"), "partition1,partition2"));
    assertEquals(
        KeyGeneratorType.NON_PARTITION,
        KeyGenUtils.inferKeyGeneratorType(Option.of("col1,col2"), ""));
    assertEquals(
        KeyGeneratorType.NON_PARTITION,
        KeyGenUtils.inferKeyGeneratorType(Option.of("col1,col2"), null));
  }

  @Test
  public void testExtractRecordKeys() {
    // test complex key form: field1:val1,field2:val2,...
    String[] s1 = KeyGenUtils.extractRecordKeys("id:1");
    Assertions.assertArrayEquals(new String[] {"1"}, s1);

    String[] s2 = KeyGenUtils.extractRecordKeys("id:1,id:2");
    Assertions.assertArrayEquals(new String[] {"1", "2"}, s2);

    String[] s3 = KeyGenUtils.extractRecordKeys("id:1,id2:__null__,id3:__empty__");
    Assertions.assertArrayEquals(new String[] {"1", null, ""}, s3);

    String[] s4 = KeyGenUtils.extractRecordKeys("id:ab:cd,id2:ef");
    Assertions.assertArrayEquals(new String[] {"ab:cd", "ef"}, s4);

    // test simple key form: val1
    String[] s5 = KeyGenUtils.extractRecordKeys("1");
    Assertions.assertArrayEquals(new String[] {"1"}, s5);

    String[] s6 = KeyGenUtils.extractRecordKeys("id:1,id2:2,2");
    Assertions.assertArrayEquals(new String[]{"1", "2", "2"}, s6);
  }

  @Test
  public void testExtractRecordKeysWithFields() {
    List<String> fields = new ArrayList<>(1);
    fields.add("id2");

    String[] s1 = KeyGenUtils.extractRecordKeysByFields("id1:1,id2:2,id3:3", fields);
    Assertions.assertArrayEquals(new String[] {"2"}, s1);

    String[] s2 = KeyGenUtils.extractRecordKeysByFields("id1:1,id2:2,2,id3:3", fields);
    Assertions.assertArrayEquals(new String[] {"2", "2"}, s2);
  }

  @Test
  void testGetRecordKey() {
    Schema nullableStringSchema = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING));
    Schema schema = Schema.createRecord("TestRecord", "doc", "test", false,
        Arrays.asList(
            new Schema.Field("key1", nullableStringSchema, "", null),
            new Schema.Field("key2", nullableStringSchema, "", null),
            new Schema.Field("key3", nullableStringSchema, "", null),
            new Schema.Field("key4", nullableStringSchema, "", null)
        ));
    GenericRecord avroRecord = new GenericData.Record(schema);
    avroRecord.put("key1", "value1");
    avroRecord.put("key2", "value2");
    avroRecord.put("key3", null);
    avroRecord.put("key4", "");

    assertEquals("key1:value1",
        KeyGenUtils.getRecordKey(avroRecord, Arrays.asList("key1"), true));
    assertThrows(HoodieKeyException.class,
        () -> KeyGenUtils.getRecordKey(avroRecord, Arrays.asList("key3"), true),
        "recordKey values: \"key3:__null__\" for fields: [key3] cannot be entirely null or empty.");
    assertThrows(HoodieKeyException.class,
        () -> KeyGenUtils.getRecordKey(avroRecord, Arrays.asList("key4"), true),
        "recordKey values: \"key4:__empty__\" for fields: [key4] cannot be entirely null or empty.");
    assertEquals("key1:value1,key2:value2",
        KeyGenUtils.getRecordKey(avroRecord, Arrays.asList("key1", "key2"), true));
    assertEquals("key1:value1,key3:__null__",
        KeyGenUtils.getRecordKey(avroRecord, Arrays.asList("key1", "key3"), true));
    assertEquals("key1:value1,key4:__empty__",
        KeyGenUtils.getRecordKey(avroRecord, Arrays.asList("key1", "key4"), true));

    assertEquals("value1",
        KeyGenUtils.getRecordKey(avroRecord, "key1", true));
    assertThrows(HoodieKeyException.class,
        () -> KeyGenUtils.getRecordKey(avroRecord, "key3", true),
        "recordKey value: \"null\" for field: \"key3\" cannot be null or empty.");
    assertThrows(HoodieKeyException.class,
        () -> KeyGenUtils.getRecordKey(avroRecord, "key4", true),
        "recordKey value: \"\" for field: \"key4\" cannot be null or empty.");
  }

  @Test
  void testIsComplexKeyGeneratorWithSingleRecordKeyField() {
    HoodieTableConfig tableConfig = new HoodieTableConfig();
    tableConfig.setValue(KEY_GENERATOR_CLASS_NAME, "org.apache.hudi.keygen.ComplexKeyGenerator");
    tableConfig.setValue(RECORDKEY_FIELDS, "id");
    assertTrue(KeyGenUtils.isComplexKeyGeneratorWithSingleRecordKeyField(tableConfig));

    tableConfig = new HoodieTableConfig();
    tableConfig.setValue(RECORDKEY_FIELDS, "userId");
    tableConfig.setValue(KEY_GENERATOR_CLASS_NAME, "org.apache.hudi.keygen.ComplexAvroKeyGenerator");
    assertTrue(KeyGenUtils.isComplexKeyGeneratorWithSingleRecordKeyField(tableConfig));
  }

  @Test
  void testIsComplexKeyGeneratorWithSingleRecordKeyFieldOnMultipleFields() {
    HoodieTableConfig tableConfig = new HoodieTableConfig();
    tableConfig.setValue(KEY_GENERATOR_CLASS_NAME, "org.apache.hudi.keygen.ComplexKeyGenerator");
    tableConfig.setValue(RECORDKEY_FIELDS, "id,userId");
    assertFalse(KeyGenUtils.isComplexKeyGeneratorWithSingleRecordKeyField(tableConfig));

    tableConfig = new HoodieTableConfig();
    tableConfig.setValue(KEY_GENERATOR_CLASS_NAME, "org.apache.hudi.keygen.ComplexAvroKeyGenerator");
    tableConfig.setValue(RECORDKEY_FIELDS, "id,userId,name");
    assertFalse(KeyGenUtils.isComplexKeyGeneratorWithSingleRecordKeyField(tableConfig));
  }

  @Test
  void testIsComplexKeyGeneratorWithSingleRecordKeyFieldOnNonComplexGenerator() {
    HoodieTableConfig tableConfig = new HoodieTableConfig();
    tableConfig.setValue(KEY_GENERATOR_CLASS_NAME, "org.apache.hudi.keygen.SimpleKeyGenerator");
    tableConfig.setValue(RECORDKEY_FIELDS, "id");
    assertFalse(KeyGenUtils.isComplexKeyGeneratorWithSingleRecordKeyField(tableConfig));

    tableConfig = new HoodieTableConfig();
    tableConfig.setValue(KEY_GENERATOR_CLASS_NAME, "org.apache.hudi.keygen.SimpleAvroKeyGenerator");
    tableConfig.setValue(RECORDKEY_FIELDS, "userId");
    assertFalse(KeyGenUtils.isComplexKeyGeneratorWithSingleRecordKeyField(tableConfig));

    tableConfig = new HoodieTableConfig();
    tableConfig.setValue(KEY_GENERATOR_CLASS_NAME, "org.apache.hudi.keygen.TimestampBasedKeyGenerator");
    tableConfig.setValue(RECORDKEY_FIELDS, "id");
    assertFalse(KeyGenUtils.isComplexKeyGeneratorWithSingleRecordKeyField(tableConfig));

    tableConfig = new HoodieTableConfig();
    tableConfig.setValue(KEY_GENERATOR_CLASS_NAME, "org.apache.hudi.keygen.CustomKeyGenerator");
    tableConfig.setValue(RECORDKEY_FIELDS, "id");
    assertFalse(KeyGenUtils.isComplexKeyGeneratorWithSingleRecordKeyField(tableConfig));
  }

  @Test
  void testIsComplexKeyGeneratorWithSingleRecordKeyFieldOnNoRecordKeyFields() {
    HoodieTableConfig tableConfig = new HoodieTableConfig();
    tableConfig.setValue(KEY_GENERATOR_CLASS_NAME, "org.apache.hudi.keygen.ComplexKeyGenerator");
    assertFalse(KeyGenUtils.isComplexKeyGeneratorWithSingleRecordKeyField(tableConfig));
  }

  @Test
  void testIsComplexKeyGeneratorWithSingleRecordKeyFieldEmptyRecordKeyFields() {
    HoodieTableConfig tableConfig = new HoodieTableConfig();
    tableConfig.setValue(KEY_GENERATOR_CLASS_NAME, "org.apache.hudi.keygen.ComplexKeyGenerator");
    tableConfig.setValue(RECORDKEY_FIELDS, "");
    assertFalse(KeyGenUtils.isComplexKeyGeneratorWithSingleRecordKeyField(tableConfig));
  }

  @Test
  void testWriteAndReadComplexKeyEncodingFile(@TempDir File tempDir) throws IOException {
    String basePath = tempDir.getAbsolutePath();
    HoodieStorage storage = HoodieTestUtils.getStorage(tempDir.toString());

    // Test writing and reading true value
    KeyGenUtils.writeComplexKeyEncodingToAuxFile(storage, basePath, true);
    Option<Boolean> result = KeyGenUtils.readComplexKeyEncodingFromAuxFile(storage, basePath);
    assertTrue(result.isPresent());
    assertTrue(result.get());

    // Test writing and reading false value
    KeyGenUtils.writeComplexKeyEncodingToAuxFile(storage, basePath, false);
    result = KeyGenUtils.readComplexKeyEncodingFromAuxFile(storage, basePath);
    assertTrue(result.isPresent());
    assertFalse(result.get());

    // Verify file location
    StoragePath auxFilePath = KeyGenUtils.getComplexKeyEncodingFilePath(basePath);
    assertTrue(storage.exists(auxFilePath));
  }

  @Test
  void testReadComplexKeyEncodingFileWhenNotExists(@TempDir File tempDir) {
    String basePath = tempDir.getAbsolutePath();
    HoodieStorage storage = HoodieTestUtils.getStorage(tempDir.toString());

    // Should return empty when file doesn't exist
    Option<Boolean> result = KeyGenUtils.readComplexKeyEncodingFromAuxFile(storage, basePath);
    assertFalse(result.isPresent());
  }

  @Test
  void testGetComplexKeyEncodingFilePath(@TempDir File tempDir) {
    String basePath = tempDir.getAbsolutePath();
    StoragePath expectedPath = new StoragePath(basePath,
        HoodieTableMetaClient.AUXILIARYFOLDER_NAME + "/" + KeyGenUtils.COMPLEX_KEY_ENCODING_FILE_NAME);
    StoragePath actualPath = KeyGenUtils.getComplexKeyEncodingFilePath(basePath);
    assertEquals(expectedPath.toString(), actualPath.toString());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testDeduceComplexKeyEncodingFromData(@TempDir File tempDir, boolean useNewEncoding) throws Exception {
    String basePath = tempDir.getAbsolutePath();
    String recordKeyFieldName = "userId";
    String partitionPath = "country=US";

    // Setup table with ComplexKeyGenerator
    Properties tableProperties = new Properties();
    tableProperties.put(HoodieTableConfig.KEY_GENERATOR_CLASS_NAME.key(), "org.apache.hudi.keygen.ComplexKeyGenerator");
    tableProperties.put(HoodieTableConfig.RECORDKEY_FIELDS.key(), recordKeyFieldName);
    tableProperties.put(HoodieTableConfig.PARTITION_FIELDS.key(), "country");
    tableProperties.put(HoodieTableConfig.VERSION.key(), "6");

    HoodieTableMetaClient metaClient = HoodieTestUtils.init(
        HoodieTestUtils.getDefaultStorageConf(), basePath, HoodieTableConfig.TABLE_TYPE_DEFAULT_VALUE, tableProperties);
    HoodieStorage storage = metaClient.getStorage();

    // Create Avro schema with Hudi meta fields
    Schema avroSchema = HoodieAvroUtils.addMetadataFields(Schema.createRecord("TestRecord", "doc", "test", false,
        Arrays.asList(
            new Schema.Field(recordKeyFieldName, Schema.create(Schema.Type.STRING), "", null),
            new Schema.Field("country", Schema.create(Schema.Type.STRING), "", null),
            new Schema.Field("name", Schema.create(Schema.Type.STRING), "", null)
        )));

    // Write parquet file with the specified encoding format
    String instantTime = "20231201120000";
    String fileName = "data_" + instantTime + "_0_0.parquet";
    StoragePath partitionDir = new StoragePath(basePath, partitionPath);
    storage.createDirectory(partitionDir);
    StoragePath filePath = new StoragePath(partitionDir, fileName);

    // Write records with appropriate _hoodie_record_key format
    writeParquetFileWithComplexKeyEncoding(filePath.toString(), avroSchema, recordKeyFieldName, useNewEncoding);

    // Create commit metadata
    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    HoodieWriteStat writeStat = new HoodieWriteStat();
    writeStat.setPath(partitionPath + "/" + fileName);
    writeStat.setNumWrites(5);
    commitMetadata.addWriteStat(partitionPath, writeStat);

    // Create commit in timeline
    HoodieActiveTimeline timeline = metaClient.getActiveTimeline();
    HoodieInstant instant = new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieActiveTimeline.COMMIT_ACTION, instantTime);
    timeline.createNewInstant(instant);
    timeline.transitionRequestedToInflight(instant, Option.empty());
    timeline.saveAsComplete(new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieActiveTimeline.COMMIT_ACTION, instantTime),
        Option.of(commitMetadata.toJsonString().getBytes()));

    // Reload metaClient to get updated timeline
    metaClient = HoodieTableMetaClient.reload(metaClient);

    // Test deduction
    boolean deducedEncoding = KeyGenUtils.deduceComplexKeyEncodingFromData(metaClient, recordKeyFieldName);
    assertEquals(useNewEncoding, deducedEncoding,
        "Should correctly deduce encoding format from parquet file (useNewEncoding=" + useNewEncoding + ")");
  }

  private void writeParquetFileWithComplexKeyEncoding(String filePath, Schema schema, String recordKeyFieldName, boolean useNewEncoding) throws Exception {
    BloomFilter filter = BloomFilterFactory.createBloomFilter(1000, 0.0001, 10000, BloomFilterTypeCode.SIMPLE.name());
    HoodieAvroWriteSupport writeSupport = new HoodieAvroWriteSupport(
        new AvroSchemaConverter().convert(schema), schema, Option.of(filter), new Properties());

    try (ParquetWriter writer = new ParquetWriter(new Path(filePath), writeSupport,
        CompressionCodecName.GZIP, 120 * 1024 * 1024, ParquetWriter.DEFAULT_PAGE_SIZE)) {
      for (int i = 0; i < 5; i++) {
        GenericRecord record = new GenericData.Record(schema);
        String recordKeyValue = "user" + i;

        // Set _hoodie_record_key based on encoding format
        String hoodieRecordKey = useNewEncoding ? recordKeyValue : (recordKeyFieldName + ":" + recordKeyValue);
        record.put(HoodieRecord.RECORD_KEY_METADATA_FIELD, hoodieRecordKey);
        record.put(HoodieRecord.COMMIT_TIME_METADATA_FIELD, "20231201120000");
        record.put(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD, "20231201120000_0_" + i);
        record.put(HoodieRecord.PARTITION_PATH_METADATA_FIELD, "country=US");
        record.put(HoodieRecord.FILENAME_METADATA_FIELD, "data_20231201120000_0_0.parquet");
        record.put(recordKeyFieldName, recordKeyValue);
        record.put("country", "US");
        record.put("name", "User " + i);

        writer.write(record);
        writeSupport.add(hoodieRecordKey);
      }
    }
  }

  @Test
  void testDeduceComplexKeyEncodingFromDataWithNoCompletedCommits(@TempDir File tempDir) throws IOException {
    String basePath = tempDir.getAbsolutePath();
    String recordKeyFieldName = "userId";

    // Setup table with ComplexKeyGenerator but no commits
    Properties tableProperties = new Properties();
    tableProperties.put(HoodieTableConfig.KEY_GENERATOR_CLASS_NAME.key(), "org.apache.hudi.keygen.ComplexKeyGenerator");
    tableProperties.put(HoodieTableConfig.RECORDKEY_FIELDS.key(), recordKeyFieldName);
    tableProperties.put(HoodieTableConfig.PARTITION_FIELDS.key(), "country");
    tableProperties.put(HoodieTableConfig.VERSION.key(), "6");

    HoodieTableMetaClient metaClient = HoodieTestUtils.init(
        HoodieTestUtils.getDefaultStorageConf(), basePath, HoodieTableConfig.TABLE_TYPE_DEFAULT_VALUE, tableProperties);

    // Should throw exception when no completed commits exist
    HoodieException exception = assertThrows(HoodieException.class,
        () -> KeyGenUtils.deduceComplexKeyEncodingFromData(metaClient, recordKeyFieldName));
    assertTrue(exception.getMessage().contains("no completed commits found"));
  }

  @Test
  void testDeduceComplexKeyEncodingFromDataWithNoBaseFiles(@TempDir File tempDir) throws IOException {
    String basePath = tempDir.getAbsolutePath();
    String recordKeyFieldName = "userId";

    // Setup table with ComplexKeyGenerator
    Properties tableProperties = new Properties();
    tableProperties.put(HoodieTableConfig.KEY_GENERATOR_CLASS_NAME.key(), "org.apache.hudi.keygen.ComplexKeyGenerator");
    tableProperties.put(HoodieTableConfig.RECORDKEY_FIELDS.key(), recordKeyFieldName);
    tableProperties.put(HoodieTableConfig.PARTITION_FIELDS.key(), "country");
    tableProperties.put(HoodieTableConfig.VERSION.key(), "6");

    HoodieTableMetaClient metaClient = HoodieTestUtils.init(
        HoodieTestUtils.getDefaultStorageConf(), basePath, HoodieTableConfig.TABLE_TYPE_DEFAULT_VALUE, tableProperties);

    // Create commit metadata with only log files (MOR scenario)
    String instantTime = "20231201120000";
    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    HoodieWriteStat writeStat = new HoodieWriteStat();
    writeStat.setPath("country=US/.data_" + instantTime + "_0_0.log.1");
    writeStat.setNumWrites(5);
    commitMetadata.addWriteStat("country=US", writeStat);

    // Create commit in timeline
    HoodieActiveTimeline timeline = metaClient.getActiveTimeline();
    HoodieInstant instant = new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieActiveTimeline.DELTA_COMMIT_ACTION, instantTime);
    timeline.createNewInstant(instant);
    timeline.transitionRequestedToInflight(instant, Option.empty());
    timeline.saveAsComplete(new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieActiveTimeline.DELTA_COMMIT_ACTION, instantTime),
        Option.of(commitMetadata.toJsonString().getBytes()));

    // Reload metaClient to get updated timeline
    metaClient = HoodieTableMetaClient.reload(metaClient);

    // Should throw exception when no base files with records are found
    HoodieException exception = assertThrows(HoodieException.class,
        () -> KeyGenUtils.deduceComplexKeyEncodingFromData(metaClient, recordKeyFieldName));
    assertTrue(exception.getMessage().contains("no base files with records found"));
  }
}
