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

package org.apache.hudi.avro;

import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.HoodieAvroRecordMerger;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.log.InstantRange;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.CloseableMappingIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.expression.Predicate;
import org.apache.hudi.io.storage.HoodieAvroFileReader;
import org.apache.hudi.io.storage.HoodieIOFactory;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

public class HoodieAvroPayloadReaderContext extends HoodieReaderContext<HoodieRecordPayload> {
  private final Map<StoragePath, HoodieAvroFileReader> reusableFileReaders;

  /**
   * Constructs an instance of the reader context that will read data into Avro records.
   * @param storageConfiguration the storage configuration to use for reading files
   * @param tableConfig the configuration of the Hudi table being read
   * @param instantRangeOpt the set of valid instants for this read
   * @param filterOpt an optional filter to apply on the record keys
   */
  public HoodieAvroPayloadReaderContext(
      StorageConfiguration<?> storageConfiguration,
      HoodieTableConfig tableConfig,
      Option<InstantRange> instantRangeOpt,
      Option<Predicate> filterOpt) {
    this(storageConfiguration, tableConfig, instantRangeOpt, filterOpt, Collections.emptyMap());
  }

  /**
   * Constructs an instance of the reader context with an optional cache of reusable file readers.
   * This provides an opportunity for increased performance when repeatedly reading from the same files.
   * The caller of this constructor is responsible for managing the lifecycle of the reusable file readers.
   * @param storageConfiguration the storage configuration to use for reading files
   * @param tableConfig the configuration of the Hudi table being read
   * @param instantRangeOpt the set of valid instants for this read
   * @param filterOpt an optional filter to apply on the record keys
   * @param reusableFileReaders a map of reusable file readers, keyed by their storage paths.
   */
  public HoodieAvroPayloadReaderContext(
      StorageConfiguration<?> storageConfiguration,
      HoodieTableConfig tableConfig,
      Option<InstantRange> instantRangeOpt,
      Option<Predicate> filterOpt,
      Map<StoragePath, HoodieAvroFileReader> reusableFileReaders) {
    super(storageConfiguration, tableConfig, instantRangeOpt, filterOpt, new AvroPayloadRecordContext(tableConfig));
    this.reusableFileReaders = reusableFileReaders;
  }

  @Override
  public ClosableIterator<HoodieRecordPayload> getFileRecordIterator(StoragePath filePath, long start, long length, Schema dataSchema,
                                                                     Schema requiredSchema, HoodieStorage storage) throws IOException {
    HoodieAvroFileReader reader;
    if (reusableFileReaders.containsKey(filePath)) {
      reader = reusableFileReaders.get(filePath);
    } else {
      reader = (HoodieAvroFileReader) HoodieIOFactory.getIOFactory(storage)
          .getReaderFactory(HoodieRecord.HoodieRecordType.AVRO).getFileReader(new HoodieConfig(),
              filePath, baseFileFormat, Option.empty());
    }
    if (keyFilterOpt.isEmpty()) {
      return new CloseableMappingIterator(reader.getIndexedRecordIterator(dataSchema, requiredSchema),
          record -> createPayload(tableConfig.getPayloadClass(), (GenericRecord) record));
    }
    if (reader.supportKeyPredicate()) {
      List<String> keys = reader.extractKeys(keyFilterOpt);
      if (!keys.isEmpty()) {
        return new CloseableMappingIterator(reader.getIndexedRecordsByKeysIterator(keys, requiredSchema),
            record -> createPayload(tableConfig.getPayloadClass(), (GenericRecord) record));
      }
    }
    if (reader.supportKeyPrefixPredicate()) {
      List<String> keyPrefixes = reader.extractKeyPrefixes(keyFilterOpt);
      if (!keyPrefixes.isEmpty()) {
        return new CloseableMappingIterator(reader.getIndexedRecordsByKeyPrefixIterator(keyPrefixes, requiredSchema),
            record -> createPayload(tableConfig.getPayloadClass(), (GenericRecord) record));
      }
    }
    return new CloseableMappingIterator(reader.getIndexedRecordIterator(dataSchema, requiredSchema),
        record -> createPayload(tableConfig.getPayloadClass(), (GenericRecord) record));
  }

  public static HoodieRecordPayload createPayload(String payloadClass, GenericRecord record)
      throws HoodieIOException {
    try {
      return (HoodieRecordPayload) ReflectionUtils.loadClass(payloadClass,
          new Class<?>[] {Option.class}, Option.of(record));
    } catch (Throwable e) {
      throw new HoodieException("Could not create payload for class: " + payloadClass, e);
    }
  }

  @Override
  protected Option<HoodieRecordMerger> getRecordMerger(RecordMergeMode mergeMode, String mergeStrategyId, String mergeImplClasses) {
    return Option.of(HoodieAvroRecordMerger.INSTANCE);
  }

  @Override
  public HoodieRecordPayload seal(HoodieRecordPayload record) {
    return record;
  }

  @Override
  public HoodieRecordPayload toBinaryRow(Schema avroSchema, HoodieRecordPayload record) {
    return null;
  }

  @Override
  public ClosableIterator<HoodieRecordPayload> mergeBootstrapReaders(ClosableIterator<HoodieRecordPayload> skeletonFileIterator, Schema skeletonRequiredSchema,
                                                                     ClosableIterator<HoodieRecordPayload> dataFileIterator, Schema dataRequiredSchema,
                                                                     List<Pair<String, Object>> requiredPartitionFieldAndValues) {
    return null;
  }

  @Override
  public UnaryOperator<HoodieRecordPayload> projectRecord(Schema from, Schema to, Map<String, String> renamedColumns) {
    return null;
  }
}
