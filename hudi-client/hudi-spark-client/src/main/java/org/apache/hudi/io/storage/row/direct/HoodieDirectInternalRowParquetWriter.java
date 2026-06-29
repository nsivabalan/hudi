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

package org.apache.hudi.io.storage.row.direct;

import org.apache.hudi.common.config.HoodieParquetConfig;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.hadoop.fs.HoodieWrapperFileSystem;
import org.apache.hudi.io.storage.row.HoodieBloomFilterRowWriteSupport;
import org.apache.hudi.io.storage.row.HoodieInternalRowFileWriter;
import org.apache.hudi.io.storage.row.HoodieRowParquetWriteSupport;
import org.apache.hudi.storage.StoragePath;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.ColumnWriteStore;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.CodecFactory;
import org.apache.parquet.hadoop.ColumnChunkPageWriteStore;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.unsafe.types.UTF8String;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Parquet implementation of {@link HoodieInternalRowFileWriter} that writes directly to a
 * {@link ColumnWriteStore} instead of going through parquet-mr's
 * {@code WriteSupport}/{@code RecordConsumer}/{@code MessageColumnIO} chain.
 *
 * <p>This writer is the fast path for immutable bulk-insert workloads. It avoids:
 * <ul>
 *   <li>per-record {@code FieldsMarker} {@link java.util.BitSet} reset and walk inside
 *       {@code MessageColumnIORecordConsumer.startMessage()}/{@code endMessage()};</li>
 *   <li>per-field {@code startField}/{@code endField} virtual dispatch through
 *       {@code MessageColumnIORecordConsumer};</li>
 *   <li>field-name string lookups and ordinal validation on every column write.</li>
 * </ul>
 * In a 10 GB local benchmark these costs accounted for ~10% of executor CPU vs Iceberg's
 * direct {@code ColumnWriteStore} writer.
 *
 * <p>For v1 only primitive types are supported (int, long, float, double, boolean, string,
 * binary). Schemas with complex types (array/map/struct/decimal/timestamp/variant) fall
 * back to the legacy {@link org.apache.hudi.io.storage.row.HoodieInternalRowParquetWriter}
 * via the factory.
 *
 * <p>The {@link HoodieRowParquetWriteSupport} class is still used here, but only to build
 * the parquet {@link MessageType} and the file-level metadata map (Spark version, time
 * zone, vector-column metadata) — its {@code RecordConsumer}-based {@code write(row)} path
 * is never invoked.
 */
public class HoodieDirectInternalRowParquetWriter
    implements HoodieInternalRowFileWriter, Closeable {

  private final ParquetFileWriter fileWriter;
  private final MessageType parquetSchema;
  private final ParquetProperties parquetProps;
  private final CodecFactory.BytesCompressor compressor;
  private final ByteBufferAllocator allocator;
  private final long targetRowGroupSize;
  private final long maxFileSize;
  private final int columnIndexTruncateLength;
  private final Map<String, String> fileMetadata;
  private final ParquetValueWriters.InternalRowStructWriter rootWriter;
  private final Option<HoodieBloomFilterRowWriteSupport> bloomFilterWriteSupportOpt;

  private ColumnChunkPageWriteStore pageStore;
  private ColumnWriteStore writeStore;
  private long recordCountInRowGroup;
  private long totalRecordCount;
  private long nextCheckRecordCount;
  private long nextCanWriteCheckRecordCount;
  private boolean cachedCanWrite = true;
  private boolean closed;

  // ParquetProperties has a small bookkeeping field "columnindex.truncate.length"; use a
  // sensible parquet-mr default rather than reading from hadoop conf, since this is internal.
  private static final int DEFAULT_COLUMN_INDEX_TRUNCATE_LENGTH = 64;

  public HoodieDirectInternalRowParquetWriter(StoragePath file,
                                              HoodieParquetConfig<HoodieRowParquetWriteSupport> parquetConfig,
                                              HoodieRowParquetWriteSupport writeSupport,
                                              ParquetValueWriters.InternalRowStructWriter rootWriter,
                                              Map<String, String> initMetadata,
                                              MessageType parquetSchema,
                                              Option<HoodieBloomFilterRowWriteSupport> bloomFilterWriteSupportOpt)
      throws IOException {
    Configuration hadoopConf = parquetConfig.getStorageConf().unwrapAs(Configuration.class);
    hadoopConf = HadoopFSUtils.registerFileSystem(file, hadoopConf);

    this.parquetSchema = parquetSchema;
    this.fileMetadata = new HashMap<>(initMetadata);
    this.rootWriter = rootWriter;
    this.bloomFilterWriteSupportOpt = bloomFilterWriteSupportOpt;
    this.columnIndexTruncateLength = DEFAULT_COLUMN_INDEX_TRUNCATE_LENGTH;
    this.targetRowGroupSize = parquetConfig.getBlockSize();

    // Same conservative ratio as HoodieBaseParquetWriter — file size check is approximate.
    this.maxFileSize = parquetConfig.getMaxFileSize()
        + Math.round(parquetConfig.getMaxFileSize() * parquetConfig.getCompressionRatio());

    this.parquetProps = ParquetProperties.builder()
        .withPageSize(parquetConfig.getPageSize())
        .withDictionaryPageSize(parquetConfig.getPageSize())
        .withDictionaryEncoding(parquetConfig.isDictionaryEnabled())
        .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0)
        .build();

    this.allocator = HeapByteBufferAllocator.getInstance();
    CodecFactory codecFactory = new CodecFactory(hadoopConf, parquetProps.getPageSizeThreshold());
    this.compressor = codecFactory.getCompressor(parquetConfig.getCompressionCodecName());

    OutputFile outputFile = HadoopOutputFile.fromPath(
        HoodieWrapperFileSystem.convertToHoodiePath(file, hadoopConf), hadoopConf);
    this.fileWriter = new ParquetFileWriter(
        outputFile, parquetSchema, ParquetFileWriter.Mode.CREATE,
        targetRowGroupSize, 0);   // maxPaddingSize=0 to match parquet-mr default for OutputFile ctor
    this.fileWriter.start();

    startRowGroup();
    this.nextCheckRecordCount = parquetProps.getMinRowCountForPageSizeCheck();
    this.nextCanWriteCheckRecordCount = ParquetProperties.DEFAULT_MINIMUM_RECORD_COUNT_FOR_CHECK;
  }

  /**
   * Helper to retrieve the {@link MessageType} that {@link HoodieRowParquetWriteSupport#init}
   * produces — Hudi's existing converter handles all the variant/vector/decimal/timestamp
   * nuance we need to preserve.
   */
  public static WriteSupport.WriteContext extractWriteContext(HoodieRowParquetWriteSupport writeSupport) {
    return writeSupport.init(writeSupport.getHadoopConf());
  }

  private void startRowGroup() {
    this.pageStore = new ColumnChunkPageWriteStore(
        compressor, parquetSchema, allocator, columnIndexTruncateLength);
    this.writeStore = parquetProps.newColumnWriteStore(parquetSchema, pageStore, pageStore);
    this.rootWriter.setColumnStore(writeStore);
    this.recordCountInRowGroup = 0;
  }

  @Override
  public void writeRow(InternalRow row) throws IOException {
    rootWriter.write(0, row);
    writeStore.endRecord();
    recordCountInRowGroup++;
    totalRecordCount++;
    if (totalRecordCount >= nextCheckRecordCount) {
      maybeFlushRowGroup();
    }
  }

  @Override
  public void writeRow(UTF8String key, InternalRow row) throws IOException {
    writeRow(row);
    if (bloomFilterWriteSupportOpt.isPresent()) {
      bloomFilterWriteSupportOpt.get().addKey(key);
    }
  }

  private void maybeFlushRowGroup() throws IOException {
    long bufferedSize = writeStore.getBufferedSize();
    long avgRecordSize = Math.max(bufferedSize / Math.max(recordCountInRowGroup, 1), 1);
    if (bufferedSize > targetRowGroupSize - 2 * avgRecordSize) {
      flushRowGroup();
    } else {
      long remainingSpace = targetRowGroupSize - bufferedSize;
      long remainingRecords = remainingSpace / avgRecordSize;
      nextCheckRecordCount = totalRecordCount + Math.min(
          Math.max(remainingRecords / 2, parquetProps.getMinRowCountForPageSizeCheck()),
          parquetProps.getMaxRowCountForPageSizeCheck());
    }
  }

  private void flushRowGroup() throws IOException {
    if (recordCountInRowGroup == 0) {
      return;
    }
    fileWriter.startBlock(recordCountInRowGroup);
    writeStore.flush();
    pageStore.flushToFileWriter(fileWriter);
    fileWriter.endBlock();
    writeStore.close();
    if (!closed) {
      startRowGroup();
      nextCheckRecordCount = totalRecordCount + parquetProps.getMinRowCountForPageSizeCheck();
    }
  }

  @Override
  public boolean canWrite() {
    // Rate-limit the actual size check — parquet's writeStore.getBufferedSize() iterates all
    // columns to sum buffered bytes, which is wasteful per-row. Mirror
    // HoodieBaseParquetWriter.canWrite()'s amortized cadence.
    if (totalRecordCount < nextCanWriteCheckRecordCount) {
      return cachedCanWrite;
    }
    long dataSize;
    try {
      dataSize = fileWriter.getPos() + writeStore.getBufferedSize();
    } catch (IOException e) {
      // If we cannot read the position, assume we can still write.
      return true;
    }
    if (totalRecordCount == 0) {
      return true;
    }
    long avgRecordSize = Math.max(dataSize / totalRecordCount, 1);
    cachedCanWrite = dataSize <= maxFileSize - avgRecordSize * 2;
    if (cachedCanWrite) {
      long remainingBytes = maxFileSize - dataSize;
      long remainingRecords = Math.max(remainingBytes / avgRecordSize, 1);
      nextCanWriteCheckRecordCount = totalRecordCount + Math.min(
          Math.max(ParquetProperties.DEFAULT_MINIMUM_RECORD_COUNT_FOR_CHECK, remainingRecords / 2),
          ParquetProperties.DEFAULT_MAXIMUM_RECORD_COUNT_FOR_CHECK);
    }
    return cachedCanWrite;
  }

  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }
    closed = true;
    if (recordCountInRowGroup > 0) {
      flushRowGroup();
    }
    if (bloomFilterWriteSupportOpt.isPresent()) {
      fileMetadata.putAll(bloomFilterWriteSupportOpt.get().finalizeMetadata());
    }
    fileWriter.end(fileMetadata);
  }
}
