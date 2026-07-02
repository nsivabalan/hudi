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

package org.apache.hudi.io.storage.row;

import org.apache.hudi.avro.HoodieBloomFilterWriteSupport;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.bloom.BloomFilterFactory;
import org.apache.hudi.common.bloom.BloomFilterTypeCode;
import org.apache.hudi.common.config.HoodieParquetConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ParquetUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.io.storage.row.direct.HoodieDirectInternalRowParquetWriter;
import org.apache.hudi.io.storage.row.direct.ParquetValueWriters;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;
import org.apache.hudi.testutils.HoodieSparkClientTestHarness;
import org.apache.hudi.testutils.SparkDatasetTestUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.schema.MessageType;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests {@link HoodieInternalRowParquetWriter} and {@link HoodieDirectInternalRowParquetWriter}.
 *
 * <p>Each test is parametrized over the two row-writer implementations:
 * <ul>
 *   <li>{@code optimizedWriter=false} — legacy WriteSupport/RecordConsumer-based writer</li>
 *   <li>{@code optimizedWriter=true} — direct ColumnWriteStore writer (
 *       see {@link HoodieDirectInternalRowParquetWriter})</li>
 * </ul>
 */
public class TestHoodieInternalRowParquetWriter extends HoodieSparkClientTestHarness {

  @BeforeEach
  public void setUp() throws Exception {
    initSparkContexts("TestHoodieInternalRowParquetWriter");
    initPath();
    initHoodieStorage();
    initTestDataGenerator();
    initMetaClient();
  }

  @AfterEach
  public void tearDown() throws Exception {
    cleanupResources();
  }

  @ParameterizedTest(name = "writeLegacyFormat={0}, optimizedWriter={1}")
  @CsvSource({"true,false", "false,false", "true,true", "false,true"})
  public void testProperWriting(boolean parquetWriteLegacyFormatEnabled,
                                boolean optimizedWriter) throws Exception {
    // Generate inputs
    Dataset<Row> inputRows = SparkDatasetTestUtils.getRandomRows(sqlContext, 100,
        HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, false);
    StructType schema = inputRows.schema();

    List<InternalRow> rows = SparkDatasetTestUtils.toInternalRows(inputRows, SparkDatasetTestUtils.ENCODER);

    HoodieWriteConfig.Builder writeConfigBuilder =
        SparkDatasetTestUtils.getConfigBuilder(basePath, timelineServicePort);

    BloomFilter bloomFilter = createBloomFilter(writeConfigBuilder);
    HoodieRowParquetWriteSupport writeSupport = getWriteSupport(
        writeConfigBuilder, storageConf.unwrap(), parquetWriteLegacyFormatEnabled, bloomFilter);
    HoodieWriteConfig cfg = writeConfigBuilder.build();
    HoodieParquetConfig<HoodieRowParquetWriteSupport> parquetConfig = new HoodieParquetConfig<>(writeSupport,
        CompressionCodecName.SNAPPY, cfg.getParquetBlockSize(), cfg.getParquetPageSize(), cfg.getParquetMaxFileSize(),
        new HadoopStorageConfiguration(writeSupport.getHadoopConf()), cfg.getParquetCompressionRatio(), cfg.parquetDictionaryEnabled());

    StoragePath filePath = new StoragePath(basePath + "/internal_row_writer.parquet");

    try (HoodieInternalRowFileWriter writer = newWriter(filePath, parquetConfig, writeSupport,
        SparkDatasetTestUtils.STRUCT_TYPE, bloomFilter, optimizedWriter)) {
      for (InternalRow row : rows) {
        writer.writeRow(row.getUTF8String(schema.fieldIndex("record_key")), row);
      }
    }

    // Step 1: Verify rows written correctly
    Dataset<Row> result = sqlContext.read().parquet(basePath);
    assertEquals(0, inputRows.except(result).count());

    // Step 2: Assert Parquet metadata was written appropriately
    List<String> recordKeys =
        rows.stream().map(r -> r.getString(schema.fieldIndex("record_key"))).collect(Collectors.toList());

    String minKey = recordKeys.stream().min(Comparator.naturalOrder()).get();
    String maxKey = recordKeys.stream().max(Comparator.naturalOrder()).get();

    FileMetaData parquetMetadata = ParquetUtils.readMetadata(storage, filePath).getFileMetaData();

    Map<String, String> extraMetadata = parquetMetadata.getKeyValueMetaData();

    assertEquals(extraMetadata.get(HoodieBloomFilterWriteSupport.HOODIE_MIN_RECORD_KEY_FOOTER), minKey);
    assertEquals(extraMetadata.get(HoodieBloomFilterWriteSupport.HOODIE_MAX_RECORD_KEY_FOOTER), maxKey);
    assertEquals(extraMetadata.get(HoodieBloomFilterWriteSupport.HOODIE_BLOOM_FILTER_TYPE_CODE), BloomFilterTypeCode.DYNAMIC_V0.name());

    // Step 3: Make sure Bloom Filter contains all the record keys
    BloomFilter readBackBloomFilter = new ParquetUtils().readBloomFilterFromMetadata(storage, filePath);
    recordKeys.forEach(recordKey -> {
      assertTrue(readBackBloomFilter.mightContain(recordKey));
    });
  }

  /**
   * Build the requested writer impl. Closing the returned writer flushes and finalizes the file.
   */
  private static HoodieInternalRowFileWriter newWriter(StoragePath filePath,
                                                       HoodieParquetConfig<HoodieRowParquetWriteSupport> parquetConfig,
                                                       HoodieRowParquetWriteSupport writeSupport,
                                                       StructType structType,
                                                       BloomFilter bloomFilter,
                                                       boolean optimizedWriter) throws java.io.IOException {
    if (!optimizedWriter) {
      return new HoodieInternalRowParquetWriter(filePath, parquetConfig);
    }
    WriteSupport.WriteContext ctx = HoodieDirectInternalRowParquetWriter.extractWriteContext(writeSupport);
    MessageType parquetSchema = ctx.getSchema();
    ParquetValueWriters.InternalRowStructWriter rootWriter =
        ParquetValueWriters.buildStruct(structType, parquetSchema);
    Option<HoodieBloomFilterRowWriteSupport> bloomFilterWriteSupportOpt =
        bloomFilter == null ? Option.empty() : Option.of(new HoodieBloomFilterRowWriteSupport(bloomFilter));
    return new HoodieDirectInternalRowParquetWriter(
        filePath, parquetConfig, writeSupport, rootWriter,
        ctx.getExtraMetaData(), parquetSchema, bloomFilterWriteSupportOpt);
  }

  private static BloomFilter createBloomFilter(HoodieWriteConfig.Builder writeConfigBuilder) {
    HoodieWriteConfig writeConfig = writeConfigBuilder.build();
    return BloomFilterFactory.createBloomFilter(
        writeConfig.getBloomFilterNumEntries(),
        writeConfig.getBloomFilterFPP(),
        writeConfig.getDynamicBloomFilterMaxNumEntries(),
        writeConfig.getBloomFilterType());
  }

  private HoodieRowParquetWriteSupport getWriteSupport(HoodieWriteConfig.Builder writeConfigBuilder,
                                                       Configuration hadoopConf,
                                                       boolean parquetWriteLegacyFormatEnabled,
                                                       BloomFilter bloomFilter) {
    writeConfigBuilder.withStorageConfig(HoodieStorageConfig.newBuilder().parquetWriteLegacyFormat(String.valueOf(parquetWriteLegacyFormatEnabled)).build());
    HoodieWriteConfig writeConfig = writeConfigBuilder.build();
    return HoodieRowParquetWriteSupport.getHoodieRowParquetWriteSupport(hadoopConf,
        SparkDatasetTestUtils.STRUCT_TYPE, Option.of(bloomFilter), writeConfig);
  }
}
