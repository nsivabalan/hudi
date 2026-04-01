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

package org.apache.hudi.client;

import org.apache.hudi.client.embedded.EmbeddedTimelineService;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.FileSystemViewStorageType;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.index.simple.HoodieSimpleIndex;
import org.apache.hudi.keygen.ComplexAvroKeyGenerator;
import org.apache.hudi.keygen.KeyGenUtils;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.table.BulkInsertPartitioner;
import org.apache.hudi.table.HoodieTable;

import org.mockito.MockedStatic;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.testutils.HoodieTestUtils.getDefaultStorageConf;
import static org.apache.hudi.testutils.Assertions.assertComplexKeyGeneratorValidationThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.when;

class TestBaseHoodieWriteClient extends HoodieCommonTestHarness {

  private static Stream<Arguments> testWithComplexKeyGeneratorValidation() {
    List<Arguments> arguments = new ArrayList<>();

    List<Arguments> keyAndPartitionFieldOptions = Arrays.asList(
        Arguments.of("r1", "p1"),
        Arguments.of("r1", "p1,p2"),
        Arguments.of("r1", ""),
        Arguments.of("r1,r2", "p1")
    );

    List<Arguments> booleanOptions = Arrays.asList(
        Arguments.of(false, true),
        Arguments.of(true, true),
        Arguments.of(true, false)
    );

    List<Integer> tableVersionOptions = Arrays.asList(6);

    arguments.addAll(Stream.of("org.apache.hudi.keygen.ComplexAvroKeyGenerator",
            "org.apache.hudi.keygen.ComplexKeyGenerator")
        .flatMap(keyGenClass -> keyAndPartitionFieldOptions.stream()
            .flatMap(keyAndPartitionField -> booleanOptions.stream()
                .flatMap(booleans -> tableVersionOptions.stream()
                    .map(tableVersion -> Arguments.of(
                        keyGenClass,
                        keyAndPartitionField.get()[0],
                        keyAndPartitionField.get()[1],
                        booleans.get()[0],
                        booleans.get()[1],
                        tableVersion
                    ))
                )
            ))
        .collect(Collectors.toList()));
    arguments.addAll(Stream.of("org.apache.hudi.keygen.SimpleAvroKeyGenerator",
            "org.apache.hudi.keygen.SimpleKeyGenerator",
            "org.apache.hudi.keygen.TimestampBasedAvroKeyGenerator",
            "org.apache.hudi.keygen.TimestampBasedKeyGenerator")
        .flatMap(keyGenClass -> booleanOptions.stream()
            .flatMap(booleans -> tableVersionOptions.stream()
                .map(tableVersion -> Arguments.of(
                    keyGenClass,
                    "r1",
                    "p1",
                    booleans.get()[0],
                    booleans.get()[1],
                    tableVersion
                ))
            )
        )
        .collect(Collectors.toList()));
    arguments.addAll(Stream.of("org.apache.hudi.keygen.NonpartitionedAvroKeyGenerator",
            "org.apache.hudi.keygen.NonpartitionedKeyGenerator")
        .flatMap(keyGenClass -> booleanOptions.stream()
            .flatMap(booleans -> tableVersionOptions.stream()
                .map(tableVersion -> Arguments.of(
                    keyGenClass,
                    "r1",
                    "",
                    booleans.get()[0],
                    booleans.get()[1],
                    tableVersion
                ))
            )
        )
        .collect(Collectors.toList()));
    arguments.addAll(Stream.of("org.apache.hudi.keygen.CustomAvroKeyGenerator",
            "org.apache.hudi.keygen.CustomKeyGenerator")
        .flatMap(keyGenClass -> booleanOptions.stream()
            .flatMap(booleans -> tableVersionOptions.stream()
                .map(tableVersion -> Arguments.of(
                    keyGenClass,
                    "r1",
                    "p1:SIMPLE",
                    booleans.get()[0],
                    booleans.get()[1],
                    tableVersion
                ))
            )
        )
        .collect(Collectors.toList()));

    return arguments.stream();
  }

  @ParameterizedTest
  @MethodSource
  void testWithComplexKeyGeneratorValidation(String keyGeneratorClass,
                                                        String recordKeyFields,
                                                        String partitionPathFields,
                                                        boolean setComplexKeyGeneratorValidationConfig,
                                                        boolean enableComplexKeyGeneratorValidation,
                                                        int tableVersion) throws IOException {
    if (basePath == null) {
      initPath();
    }
    Properties tableProperties = new Properties();
    tableProperties.put(HoodieTableConfig.KEY_GENERATOR_CLASS_NAME.key(), keyGeneratorClass);
    tableProperties.put(HoodieTableConfig.RECORDKEY_FIELDS.key(), recordKeyFields);
    tableProperties.put(HoodieTableConfig.PARTITION_FIELDS.key(), partitionPathFields);
    tableProperties.put(HoodieTableConfig.VERSION.key(), String.valueOf(tableVersion));
    Properties writeProperties = new Properties();
    writeProperties.put(HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key(), keyGeneratorClass);
    writeProperties.put(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), recordKeyFields);
    writeProperties.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), partitionPathFields);
    if (setComplexKeyGeneratorValidationConfig) {
      writeProperties.put(
          HoodieWriteConfig.ENABLE_COMPLEX_KEYGEN_VALIDATION.key(), enableComplexKeyGeneratorValidation);
    }
    metaClient = HoodieTestUtils.init(
        HoodieTestUtils.getDefaultStorageConf(), basePath, getTableType(), tableProperties);
    HoodieWriteConfig.Builder writeConfigBuilder = HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withProperties(writeProperties);
    HoodieTable<String, String, String, String> table = mock(HoodieTable.class);
    BaseHoodieTableServiceClient<String, String, String> tableServiceClient = mock(BaseHoodieTableServiceClient.class);
    TestWriteClient writeClient = new TestWriteClient(writeConfigBuilder.build(), table, Option.empty(), tableServiceClient);

    if (tableVersion <= 8 && enableComplexKeyGeneratorValidation
        && (ComplexAvroKeyGenerator.class.getCanonicalName().equals(keyGeneratorClass)
        || "org.apache.hudi.keygen.ComplexKeyGenerator".equals(keyGeneratorClass))
        && recordKeyFields.split(",").length == 1) {
      assertComplexKeyGeneratorValidationThrows(() -> writeClient.initTable(WriteOperationType.INSERT, Option.empty()), "ingestion");
    } else {
      writeClient.initTable(WriteOperationType.INSERT, Option.empty());
      String requestedTime = writeClient.startCommit();

      HoodieTimeline writeTimeline = metaClient.getActiveTimeline().getWriteTimeline();
      assertTrue(writeTimeline.lastInstant().isPresent());
      assertEquals("commit", writeTimeline.lastInstant().get().getAction());
      assertEquals(requestedTime, writeTimeline.lastInstant().get().getTimestamp());
    }
  }

  private static class TestWriteClient extends BaseHoodieWriteClient<String, String, String, String> {
    private final HoodieTable<String, String, String, String> table;

    public TestWriteClient(HoodieWriteConfig writeConfig, HoodieTable<String, String, String, String> table, Option<EmbeddedTimelineService> timelineService,
                           BaseHoodieTableServiceClient<String, String, String> tableServiceClient) {
      super(new HoodieLocalEngineContext(getDefaultStorageConf()), writeConfig, timelineService, null);
      this.table = table;
      this.tableServiceClient = tableServiceClient;
    }

    @Override
    protected HoodieIndex<?, ?> createIndex(HoodieWriteConfig writeConfig) {
      return new HoodieSimpleIndex(config, Option.empty());
    }

    @Override
    public boolean commit(String instantTime, String writeStatuses, Option<Map<String, String>> extraMetadata, String commitActionType, Map<String, List<String>> partitionToReplacedFileIds,
                          Option<BiConsumer<HoodieTableMetaClient, HoodieCommitMetadata>> extraPreCommitFunc) {
      return false;
    }

    @Override
    protected HoodieTable<String, String, String, String> createTable(HoodieWriteConfig config, Configuration hadoopConf) {
      // table should only be made with remote view config for these tests
      FileSystemViewStorageType storageType = config.getViewStorageConfig().getStorageType();
      Assertions.assertTrue(storageType == FileSystemViewStorageType.REMOTE_FIRST || storageType == FileSystemViewStorageType.REMOTE_ONLY);
      return table;
    }

    @Override
    protected HoodieTable<String, String, String, String> createTable(HoodieWriteConfig config, Configuration hadoopConf, HoodieTableMetaClient metaClient) {
      // table should only be made with remote view config for these tests
      FileSystemViewStorageType storageType = config.getViewStorageConfig().getStorageType();
      Assertions.assertTrue(storageType == FileSystemViewStorageType.REMOTE_FIRST || storageType == FileSystemViewStorageType.REMOTE_ONLY);
      // Ensure the returned table has the correct metaClient
      when(table.getMetaClient()).thenReturn(metaClient);
      return table;
    }

    @Override
    protected void validateTimestamp(HoodieTableMetaClient metaClient, String instantTime) {
    }

    @Override
    public String filterExists(String hoodieRecords) {
      return "";
    }

    @Override
    public String upsert(String records, String instantTime) {
      return "";
    }

    @Override
    public String upsertPreppedRecords(String preppedRecords, String instantTime) {
      return "";
    }

    @Override
    public String insert(String records, String instantTime) {
      return "";
    }

    @Override
    public String insertPreppedRecords(String preppedRecords, String instantTime) {
      return "";
    }

    @Override
    public String bulkInsert(String records, String instantTime) {
      return "";
    }

    @Override
    public String bulkInsert(String records, String instantTime, Option<BulkInsertPartitioner> userDefinedBulkInsertPartitioner) {
      return "";
    }

    @Override
    public String bulkInsertPreppedRecords(String preppedRecords, String instantTime, Option<BulkInsertPartitioner> bulkInsertPartitioner) {
      return "";
    }

    @Override
    public String delete(String keys, String instantTime) {
      return "";
    }

    @Override
    public String deletePrepped(String preppedRecords, String instantTime) {
      return "";
    }
  }

  @ParameterizedTest
  @MethodSource("testAutoDeductionEnabledWithNoAuxFileParams")
  void testAutoDeductionEnabledWithNoAuxFile(String keyGeneratorClass, boolean deducedEncoding) throws IOException {
    if (basePath == null) {
      initPath();
    }

    // Setup table with complex key generator and single record key field
    Properties tableProperties = new Properties();
    tableProperties.put(HoodieTableConfig.KEY_GENERATOR_CLASS_NAME.key(), keyGeneratorClass);
    tableProperties.put(HoodieTableConfig.RECORDKEY_FIELDS.key(), "userId");
    tableProperties.put(HoodieTableConfig.PARTITION_FIELDS.key(), "country");
    tableProperties.put(HoodieTableConfig.VERSION.key(), "6");

    metaClient = HoodieTestUtils.init(
        HoodieTestUtils.getDefaultStorageConf(), basePath, getTableType(), tableProperties);

    // Setup write config with auto-deduction enabled
    Properties writeProperties = new Properties();
    writeProperties.put(HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key(), keyGeneratorClass);
    writeProperties.put(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "userId");
    writeProperties.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "country");
    writeProperties.put(HoodieWriteConfig.COMPLEX_KEYGEN_AUTO_DEDUCE_ENCODING.key(), "true");

    HoodieWriteConfig.Builder writeConfigBuilder = HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withProperties(writeProperties);

    HoodieTable<String, String, String, String> table = mock(HoodieTable.class);
    BaseHoodieTableServiceClient<String, String, String> tableServiceClient = mock(BaseHoodieTableServiceClient.class);

    // Mock KeyGenUtils static methods
    try (MockedStatic<KeyGenUtils> keyGenUtilsMock = mockStatic(KeyGenUtils.class)) {
      // No aux file exists
      keyGenUtilsMock.when(() -> KeyGenUtils.readComplexKeyEncodingFromAuxFile(any(), anyString()))
          .thenReturn(Option.empty());

      // Deduction returns the specified value
      keyGenUtilsMock.when(() -> KeyGenUtils.deduceComplexKeyEncodingFromData(any(), eq("userId")))
          .thenReturn(deducedEncoding);

      // Allow actual writeComplexKeyEncodingToAuxFile to be called (void method)
      keyGenUtilsMock.when(() -> KeyGenUtils.writeComplexKeyEncodingToAuxFile(any(), anyString(), eq(deducedEncoding)))
          .then(invocation -> null);

      // Allow isComplexKeyGeneratorWithSingleRecordKeyField to use actual implementation
      keyGenUtilsMock.when(() -> KeyGenUtils.isComplexKeyGeneratorWithSingleRecordKeyField(any()))
          .thenCallRealMethod();

      TestWriteClient writeClient = new TestWriteClient(writeConfigBuilder.build(), table, Option.empty(), tableServiceClient);

      // Should succeed without throwing validation error
      writeClient.initTable(WriteOperationType.INSERT, Option.empty());

      // Verify deduction was called
      keyGenUtilsMock.verify(() -> KeyGenUtils.deduceComplexKeyEncodingFromData(any(), eq("userId")));

      // Verify aux file was written
      keyGenUtilsMock.verify(() -> KeyGenUtils.writeComplexKeyEncodingToAuxFile(any(), anyString(), eq(deducedEncoding)));

      // Verify config was set correctly
      assertEquals(deducedEncoding, writeClient.getConfig().useComplexKeygenNewEncoding());
    }
  }

  private static Stream<Arguments> testAutoDeductionEnabledWithNoAuxFileParams() {
    return Stream.of(
        Arguments.of("org.apache.hudi.keygen.ComplexKeyGenerator", true),
        Arguments.of("org.apache.hudi.keygen.ComplexKeyGenerator", false),
        Arguments.of("org.apache.hudi.keygen.ComplexAvroKeyGenerator", true),
        Arguments.of("org.apache.hudi.keygen.ComplexAvroKeyGenerator", false)
    );
  }

  @ParameterizedTest
  @MethodSource("testAutoDeductionEnabledWithExistingAuxFileParams")
  void testAutoDeductionEnabledWithExistingAuxFile(String keyGeneratorClass, boolean cachedEncoding) throws IOException {
    if (basePath == null) {
      initPath();
    }

    // Setup table with complex key generator and single record key field
    Properties tableProperties = new Properties();
    tableProperties.put(HoodieTableConfig.KEY_GENERATOR_CLASS_NAME.key(), keyGeneratorClass);
    tableProperties.put(HoodieTableConfig.RECORDKEY_FIELDS.key(), "userId");
    tableProperties.put(HoodieTableConfig.PARTITION_FIELDS.key(), "country");
    tableProperties.put(HoodieTableConfig.VERSION.key(), "6");

    metaClient = HoodieTestUtils.init(
        HoodieTestUtils.getDefaultStorageConf(), basePath, getTableType(), tableProperties);

    // Setup write config with auto-deduction enabled
    Properties writeProperties = new Properties();
    writeProperties.put(HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key(), keyGeneratorClass);
    writeProperties.put(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "userId");
    writeProperties.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "country");
    writeProperties.put(HoodieWriteConfig.COMPLEX_KEYGEN_AUTO_DEDUCE_ENCODING.key(), "true");

    HoodieWriteConfig.Builder writeConfigBuilder = HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withProperties(writeProperties);

    HoodieTable<String, String, String, String> table = mock(HoodieTable.class);
    BaseHoodieTableServiceClient<String, String, String> tableServiceClient = mock(BaseHoodieTableServiceClient.class);

    // Mock KeyGenUtils static methods
    try (MockedStatic<KeyGenUtils> keyGenUtilsMock = mockStatic(KeyGenUtils.class)) {
      // Aux file exists with cached value
      keyGenUtilsMock.when(() -> KeyGenUtils.readComplexKeyEncodingFromAuxFile(any(), anyString()))
          .thenReturn(Option.of(cachedEncoding));

      // Allow isComplexKeyGeneratorWithSingleRecordKeyField to use actual implementation
      keyGenUtilsMock.when(() -> KeyGenUtils.isComplexKeyGeneratorWithSingleRecordKeyField(any()))
          .thenCallRealMethod();

      TestWriteClient writeClient = new TestWriteClient(writeConfigBuilder.build(), table, Option.empty(), tableServiceClient);

      // Should succeed without throwing validation error
      writeClient.initTable(WriteOperationType.INSERT, Option.empty());

      // Verify deduction was NOT called (used cached value)
      keyGenUtilsMock.verify(() -> KeyGenUtils.deduceComplexKeyEncodingFromData(any(), anyString()), never());

      // Verify aux file was NOT written (already exists)
      keyGenUtilsMock.verify(() -> KeyGenUtils.writeComplexKeyEncodingToAuxFile(any(), anyString(), eq(cachedEncoding)), never());

      // Verify config was set correctly from cached value
      assertEquals(cachedEncoding, writeClient.getConfig().useComplexKeygenNewEncoding());
    }
  }

  private static Stream<Arguments> testAutoDeductionEnabledWithExistingAuxFileParams() {
    return Stream.of(
        Arguments.of("org.apache.hudi.keygen.ComplexKeyGenerator", true),
        Arguments.of("org.apache.hudi.keygen.ComplexKeyGenerator", false),
        Arguments.of("org.apache.hudi.keygen.ComplexAvroKeyGenerator", true),
        Arguments.of("org.apache.hudi.keygen.ComplexAvroKeyGenerator", false)
    );
  }

  @ParameterizedTest
  @MethodSource("testAutoDeductionDisabledWithValidationEnabledParams")
  void testAutoDeductionDisabledWithValidationEnabled(String keyGeneratorClass) throws IOException {
    if (basePath == null) {
      initPath();
    }

    // Setup table with complex key generator and single record key field
    Properties tableProperties = new Properties();
    tableProperties.put(HoodieTableConfig.KEY_GENERATOR_CLASS_NAME.key(), keyGeneratorClass);
    tableProperties.put(HoodieTableConfig.RECORDKEY_FIELDS.key(), "userId");
    tableProperties.put(HoodieTableConfig.PARTITION_FIELDS.key(), "country");
    tableProperties.put(HoodieTableConfig.VERSION.key(), "6");

    metaClient = HoodieTestUtils.init(
        HoodieTestUtils.getDefaultStorageConf(), basePath, getTableType(), tableProperties);

    // Setup write config with auto-deduction disabled and validation enabled
    Properties writeProperties = new Properties();
    writeProperties.put(HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key(), keyGeneratorClass);
    writeProperties.put(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "userId");
    writeProperties.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "country");
    writeProperties.put(HoodieWriteConfig.COMPLEX_KEYGEN_AUTO_DEDUCE_ENCODING.key(), "false");
    writeProperties.put(HoodieWriteConfig.ENABLE_COMPLEX_KEYGEN_VALIDATION.key(), "true");

    HoodieWriteConfig.Builder writeConfigBuilder = HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withProperties(writeProperties);

    HoodieTable<String, String, String, String> table = mock(HoodieTable.class);
    BaseHoodieTableServiceClient<String, String, String> tableServiceClient = mock(BaseHoodieTableServiceClient.class);

    TestWriteClient writeClient = new TestWriteClient(writeConfigBuilder.build(), table, Option.empty(), tableServiceClient);

    // Should throw validation error
    assertComplexKeyGeneratorValidationThrows(
        () -> writeClient.initTable(WriteOperationType.INSERT, Option.empty()), "ingestion");
  }

  private static Stream<Arguments> testAutoDeductionDisabledWithValidationEnabledParams() {
    return Stream.of(
        Arguments.of("org.apache.hudi.keygen.ComplexKeyGenerator"),
        Arguments.of("org.apache.hudi.keygen.ComplexAvroKeyGenerator")
    );
  }

  @ParameterizedTest
  @MethodSource("testAutoDeductionDisabledWithValidationDisabledParams")
  void testAutoDeductionDisabledWithValidationDisabled(String keyGeneratorClass) throws IOException {
    if (basePath == null) {
      initPath();
    }

    // Setup table with complex key generator and single record key field
    Properties tableProperties = new Properties();
    tableProperties.put(HoodieTableConfig.KEY_GENERATOR_CLASS_NAME.key(), keyGeneratorClass);
    tableProperties.put(HoodieTableConfig.RECORDKEY_FIELDS.key(), "userId");
    tableProperties.put(HoodieTableConfig.PARTITION_FIELDS.key(), "country");
    tableProperties.put(HoodieTableConfig.VERSION.key(), "6");

    metaClient = HoodieTestUtils.init(
        HoodieTestUtils.getDefaultStorageConf(), basePath, getTableType(), tableProperties);

    // Setup write config with auto-deduction disabled and validation disabled
    Properties writeProperties = new Properties();
    writeProperties.put(HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key(), keyGeneratorClass);
    writeProperties.put(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "userId");
    writeProperties.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "country");
    writeProperties.put(HoodieWriteConfig.COMPLEX_KEYGEN_AUTO_DEDUCE_ENCODING.key(), "false");
    writeProperties.put(HoodieWriteConfig.ENABLE_COMPLEX_KEYGEN_VALIDATION.key(), "false");

    HoodieWriteConfig.Builder writeConfigBuilder = HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withProperties(writeProperties);

    HoodieTable<String, String, String, String> table = mock(HoodieTable.class);
    BaseHoodieTableServiceClient<String, String, String> tableServiceClient = mock(BaseHoodieTableServiceClient.class);

    TestWriteClient writeClient = new TestWriteClient(writeConfigBuilder.build(), table, Option.empty(), tableServiceClient);

    // Should succeed without throwing validation error
    writeClient.initTable(WriteOperationType.INSERT, Option.empty());
    String requestedTime = writeClient.startCommit();

    HoodieTimeline writeTimeline = metaClient.getActiveTimeline().getWriteTimeline();
    assertTrue(writeTimeline.lastInstant().isPresent());
    assertEquals("commit", writeTimeline.lastInstant().get().getAction());
    assertEquals(requestedTime, writeTimeline.lastInstant().get().getTimestamp());
  }

  private static Stream<Arguments> testAutoDeductionDisabledWithValidationDisabledParams() {
    return Stream.of(
        Arguments.of("org.apache.hudi.keygen.ComplexKeyGenerator"),
        Arguments.of("org.apache.hudi.keygen.ComplexAvroKeyGenerator")
    );
  }

  @ParameterizedTest
  @MethodSource("testAutoDeductionWithNonComplexKeyGenParams")
  void testAutoDeductionWithNonComplexKeyGen(String keyGeneratorClass) throws IOException {
    if (basePath == null) {
      initPath();
    }

    // Setup table with non-complex key generator
    Properties tableProperties = new Properties();
    tableProperties.put(HoodieTableConfig.KEY_GENERATOR_CLASS_NAME.key(), keyGeneratorClass);
    tableProperties.put(HoodieTableConfig.RECORDKEY_FIELDS.key(), "userId");
    tableProperties.put(HoodieTableConfig.PARTITION_FIELDS.key(), "country");
    tableProperties.put(HoodieTableConfig.VERSION.key(), "6");

    metaClient = HoodieTestUtils.init(
        HoodieTestUtils.getDefaultStorageConf(), basePath, getTableType(), tableProperties);

    // Setup write config with auto-deduction enabled
    Properties writeProperties = new Properties();
    writeProperties.put(HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key(), keyGeneratorClass);
    writeProperties.put(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "userId");
    writeProperties.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "country");
    writeProperties.put(HoodieWriteConfig.COMPLEX_KEYGEN_AUTO_DEDUCE_ENCODING.key(), "true");

    HoodieWriteConfig.Builder writeConfigBuilder = HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withProperties(writeProperties);

    HoodieTable<String, String, String, String> table = mock(HoodieTable.class);
    BaseHoodieTableServiceClient<String, String, String> tableServiceClient = mock(BaseHoodieTableServiceClient.class);

    // Mock KeyGenUtils static methods
    try (MockedStatic<KeyGenUtils> keyGenUtilsMock = mockStatic(KeyGenUtils.class)) {
      // Allow isComplexKeyGeneratorWithSingleRecordKeyField to use actual implementation
      keyGenUtilsMock.when(() -> KeyGenUtils.isComplexKeyGeneratorWithSingleRecordKeyField(any()))
          .thenCallRealMethod();

      TestWriteClient writeClient = new TestWriteClient(writeConfigBuilder.build(), table, Option.empty(), tableServiceClient);

      // Should succeed without auto-deduction logic being triggered
      writeClient.initTable(WriteOperationType.INSERT, Option.empty());

      // Verify deduction was NOT called (not a complex keygen with single field)
      keyGenUtilsMock.verify(() -> KeyGenUtils.deduceComplexKeyEncodingFromData(any(), anyString()), never());
      keyGenUtilsMock.verify(() -> KeyGenUtils.readComplexKeyEncodingFromAuxFile(any(), anyString()), never());
      keyGenUtilsMock.verify(() -> KeyGenUtils.writeComplexKeyEncodingToAuxFile(any(), anyString(), eq(true)), never());
    }
  }

  private static Stream<Arguments> testAutoDeductionWithNonComplexKeyGenParams() {
    return Stream.of(
        Arguments.of("org.apache.hudi.keygen.SimpleKeyGenerator"),
        Arguments.of("org.apache.hudi.keygen.SimpleAvroKeyGenerator"),
        Arguments.of("org.apache.hudi.keygen.TimestampBasedKeyGenerator"),
        Arguments.of("org.apache.hudi.keygen.NonpartitionedKeyGenerator")
    );
  }

  @ParameterizedTest
  @MethodSource("testAutoDeductionWithComplexKeyGenMultipleFieldsParams")
  void testAutoDeductionWithComplexKeyGenMultipleFields(String keyGeneratorClass, String recordKeyFields) throws IOException {
    if (basePath == null) {
      initPath();
    }

    // Setup table with complex key generator but multiple record key fields
    Properties tableProperties = new Properties();
    tableProperties.put(HoodieTableConfig.KEY_GENERATOR_CLASS_NAME.key(), keyGeneratorClass);
    tableProperties.put(HoodieTableConfig.RECORDKEY_FIELDS.key(), recordKeyFields);
    tableProperties.put(HoodieTableConfig.PARTITION_FIELDS.key(), "country");
    tableProperties.put(HoodieTableConfig.VERSION.key(), "6");

    metaClient = HoodieTestUtils.init(
        HoodieTestUtils.getDefaultStorageConf(), basePath, getTableType(), tableProperties);

    // Setup write config with auto-deduction enabled
    Properties writeProperties = new Properties();
    writeProperties.put(HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key(), keyGeneratorClass);
    writeProperties.put(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), recordKeyFields);
    writeProperties.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "country");
    writeProperties.put(HoodieWriteConfig.COMPLEX_KEYGEN_AUTO_DEDUCE_ENCODING.key(), "true");

    HoodieWriteConfig.Builder writeConfigBuilder = HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withProperties(writeProperties);

    HoodieTable<String, String, String, String> table = mock(HoodieTable.class);
    BaseHoodieTableServiceClient<String, String, String> tableServiceClient = mock(BaseHoodieTableServiceClient.class);

    // Mock KeyGenUtils static methods
    try (MockedStatic<KeyGenUtils> keyGenUtilsMock = mockStatic(KeyGenUtils.class)) {
      // Allow isComplexKeyGeneratorWithSingleRecordKeyField to use actual implementation
      keyGenUtilsMock.when(() -> KeyGenUtils.isComplexKeyGeneratorWithSingleRecordKeyField(any()))
          .thenCallRealMethod();

      TestWriteClient writeClient = new TestWriteClient(writeConfigBuilder.build(), table, Option.empty(), tableServiceClient);

      // Should succeed without auto-deduction logic being triggered
      writeClient.initTable(WriteOperationType.INSERT, Option.empty());

      // Verify deduction was NOT called (multiple record key fields)
      keyGenUtilsMock.verify(() -> KeyGenUtils.deduceComplexKeyEncodingFromData(any(), anyString()), never());
      keyGenUtilsMock.verify(() -> KeyGenUtils.readComplexKeyEncodingFromAuxFile(any(), anyString()), never());
      keyGenUtilsMock.verify(() -> KeyGenUtils.writeComplexKeyEncodingToAuxFile(any(), anyString(), eq(true)), never());
    }
  }

  private static Stream<Arguments> testAutoDeductionWithComplexKeyGenMultipleFieldsParams() {
    return Stream.of(
        Arguments.of("org.apache.hudi.keygen.ComplexKeyGenerator", "userId,orderId"),
        Arguments.of("org.apache.hudi.keygen.ComplexAvroKeyGenerator", "userId,orderId"),
        Arguments.of("org.apache.hudi.keygen.ComplexKeyGenerator", "id1,id2,id3")
    );
  }
}