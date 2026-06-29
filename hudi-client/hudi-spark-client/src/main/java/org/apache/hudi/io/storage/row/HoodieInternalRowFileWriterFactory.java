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

import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.bloom.BloomFilterFactory;
import org.apache.hudi.common.config.HoodieParquetConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.engine.LocalTaskContextSupplier;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.io.storage.HoodieSparkLanceWriter;
import org.apache.hudi.io.storage.row.direct.HoodieDirectInternalRowParquetWriter;
import org.apache.hudi.io.storage.row.direct.ParquetValueWriters;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;
import org.apache.hudi.table.HoodieTable;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.schema.MessageType;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.hudi.common.model.HoodieFileFormat.LANCE;
import static org.apache.hudi.common.model.HoodieFileFormat.PARQUET;
import static org.apache.hudi.common.util.ParquetUtils.getCompressionCodecName;

/**
 * Factory to assist in instantiating a new {@link HoodieInternalRowFileWriter}.
 */
public class HoodieInternalRowFileWriterFactory {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieInternalRowFileWriterFactory.class);

  /**
   * Factory method to assist in instantiating an instance of {@link HoodieInternalRowFileWriter}.
   * @param path path of the RowFileWriter.
   * @param hoodieTable instance of {@link HoodieTable} in use.
   * @param writeConfig instance of {@link HoodieWriteConfig} to use.
   * @param schema schema of the dataset in use.
   * @return the instantiated {@link HoodieInternalRowFileWriter}.
   * @throws IOException if format is not supported or if any exception during instantiating the RowFileWriter.
   *
   */
  public static HoodieInternalRowFileWriter getInternalRowFileWriter(StoragePath path,
                                                                     HoodieTable hoodieTable,
                                                                     HoodieWriteConfig writeConfig,
                                                                     StructType schema)
      throws IOException {
    final String extension = FSUtils.getFileExtension(path.getName());
    if (PARQUET.getFileExtension().equals(extension)) {
      return newParquetInternalRowFileWriter(path, hoodieTable, writeConfig, schema, tryInstantiateBloomFilter(writeConfig));
    } else if (LANCE.getFileExtension().equals(extension)) {
      long maxFileSize = writeConfig.getLongOrDefault(HoodieStorageConfig.LANCE_MAX_FILE_SIZE);
      long allocatorSize = writeConfig.getLongOrDefault(HoodieStorageConfig.LANCE_WRITE_ALLOCATOR_SIZE_BYTES);
      long flushByteWatermark = writeConfig.getLongOrDefault(HoodieStorageConfig.LANCE_WRITE_FLUSH_BYTE_WATERMARK);
      return newLanceInternalRowFileWriter(path, hoodieTable, schema, maxFileSize, allocatorSize, flushByteWatermark);
    }
    throw new UnsupportedOperationException(extension + " format not supported yet.");
  }

  private static HoodieInternalRowFileWriter newParquetInternalRowFileWriter(StoragePath path,
                                                                             HoodieTable table,
                                                                             HoodieWriteConfig writeConfig,
                                                                             StructType structType,
                                                                             Option<BloomFilter> bloomFilterOpt
  )
      throws IOException {
    HoodieRowParquetWriteSupport writeSupport = HoodieRowParquetWriteSupport
        .getHoodieRowParquetWriteSupport((Configuration) table.getStorageConf().unwrap(), structType, bloomFilterOpt, writeConfig);

    HoodieParquetConfig<HoodieRowParquetWriteSupport> parquetConfig = new HoodieParquetConfig<>(
        writeSupport,
        getCompressionCodecName(writeConfig.getParquetCompressionCodec()),
        writeConfig.getParquetBlockSize(),
        writeConfig.getParquetPageSize(),
        writeConfig.getParquetMaxFileSize(),
        new HadoopStorageConfiguration(writeSupport.getHadoopConf()),
        writeConfig.getParquetCompressionRatio(),
        writeConfig.parquetDictionaryEnabled());

    if (writeConfig.getBooleanOrDefault(HoodieStorageConfig.OPTIMIZED_ROW_WRITER_ENABLE)) {
      HoodieInternalRowFileWriter optimized = tryNewOptimizedWriter(
          path, parquetConfig, writeSupport, structType, bloomFilterOpt);
      if (optimized != null) {
        return optimized;
      }
      // Fell back — fall through to the legacy writer.
    }

    return new HoodieInternalRowParquetWriter(path, parquetConfig);
  }

  /**
   * Attempt to construct the optimized {@link HoodieDirectInternalRowParquetWriter}.
   * Returns {@code null} if the schema uses a type the optimized writer does not yet
   * support, so the caller can fall back to the legacy WriteSupport-based writer.
   */
  private static HoodieInternalRowFileWriter tryNewOptimizedWriter(
      StoragePath path,
      HoodieParquetConfig<HoodieRowParquetWriteSupport> parquetConfig,
      HoodieRowParquetWriteSupport writeSupport,
      StructType structType,
      Option<BloomFilter> bloomFilterOpt) throws IOException {
    // Use the existing schema converter via WriteSupport.init() — this preserves all the
    // variant/vector/decimal/timestamp nuance even though we never call the RecordConsumer
    // write() path.
    WriteSupport.WriteContext ctx = HoodieDirectInternalRowParquetWriter.extractWriteContext(writeSupport);
    MessageType parquetSchema = ctx.getSchema();
    ParquetValueWriters.InternalRowStructWriter rootWriter;
    try {
      rootWriter = ParquetValueWriters.buildStruct(structType, parquetSchema);
    } catch (UnsupportedOperationException e) {
      LOG.info("Optimized row writer not applicable for schema (falling back to legacy writer): {}",
          e.getMessage());
      return null;
    }
    Option<HoodieBloomFilterRowWriteSupport> bloomFilterWriteSupportOpt =
        bloomFilterOpt.map(HoodieBloomFilterRowWriteSupport::new);
    LOG.info("Using optimized direct-ColumnWriteStore parquet writer for path {}", path);
    return new HoodieDirectInternalRowParquetWriter(
        path, parquetConfig, writeSupport, rootWriter,
        ctx.getExtraMetaData(), parquetSchema, bloomFilterWriteSupportOpt);
  }

  private static HoodieInternalRowFileWriter newLanceInternalRowFileWriter(StoragePath path,
                                                                           HoodieTable table,
                                                                           StructType structType,
                                                                           long maxFileSize,
                                                                           long allocatorSize,
                                                                           long flushByteWatermark)
      throws IOException {
    return HoodieSparkLanceWriter.builder()
        .file(path)
        .sparkSchema(structType)
        .taskContextSupplier(new LocalTaskContextSupplier())
        .storage(table.getStorage())
        .maxFileSize(maxFileSize)
        .allocatorSize(allocatorSize)
        .flushByteWatermark(flushByteWatermark)
        .build();
  }

  private static Option<BloomFilter> tryInstantiateBloomFilter(HoodieWriteConfig writeConfig) {
    // NOTE: Currently Bloom Filter is only going to be populated if meta-fields are populated
    if (writeConfig.populateMetaFields()) {
      BloomFilter bloomFilter = BloomFilterFactory.createBloomFilter(
          writeConfig.getBloomFilterNumEntries(),
          writeConfig.getBloomFilterFPP(),
          writeConfig.getDynamicBloomFilterMaxNumEntries(),
          writeConfig.getBloomFilterType());

      return Option.of(bloomFilter);
    }

    return Option.empty();
  }
}
