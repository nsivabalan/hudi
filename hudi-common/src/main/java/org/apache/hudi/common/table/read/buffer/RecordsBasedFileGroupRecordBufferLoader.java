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

package org.apache.hudi.common.table.read.buffer;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.PartialUpdateMode;
import org.apache.hudi.common.table.read.BaseFileUpdateCallback;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.table.read.HoodieReadStats;
import org.apache.hudi.common.table.read.InputSplit;
import org.apache.hudi.common.table.read.ReaderParameters;
import org.apache.hudi.common.table.read.UpdateProcessor;
import org.apache.hudi.common.util.DefaultSizeEstimator;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.storage.HoodieStorage;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.apache.hudi.common.config.HoodieCommonConfig.DISK_MAP_BITCASK_COMPRESSION_ENABLED;
import static org.apache.hudi.common.config.HoodieCommonConfig.SPILLABLE_DISK_MAP_TYPE;
import static org.apache.hudi.common.config.HoodieMemoryConfig.MAX_MEMORY_FOR_MERGE;
import static org.apache.hudi.common.config.HoodieMemoryConfig.SPILLABLE_MAP_BASE_PATH;

public class RecordsBasedFileGroupRecordBufferLoader<T> implements FileGroupRecordBufferLoader<T> {

  private final Map<String, HoodieRecord<T>> keyToNewRecords;
  protected ExternalSpillableMap<Serializable, BufferedRecord<T>> records;

  public RecordsBasedFileGroupRecordBufferLoader(Map<String, HoodieRecord<T>> keyToNewRecords) {
    this.keyToNewRecords = keyToNewRecords;
  }

  @Override
  public boolean hasLogFiles() {
    return false;
  }

  @Override
  public Pair<HoodieFileGroupRecordBuffer<T>, List<String>> getRecordBuffer(HoodieReaderContext<T> readerContext, HoodieStorage storage, InputSplit inputSplit, List<String> orderingFieldNames,
                                                                            HoodieTableMetaClient hoodieTableMetaClient, TypedProperties props, ReaderParameters readerParameters,
                                                                            HoodieReadStats readStats, Option<BaseFileUpdateCallback<T>> fileGroupUpdateCallback) {

    PartialUpdateMode partialUpdateMode = hoodieTableMetaClient.getTableConfig().getPartialUpdateMode();
    UpdateProcessor<T> updateProcessor = UpdateProcessor.create(readStats, readerContext, readerParameters.emitDeletes(), fileGroupUpdateCallback);

    try {
      records = initializeRecordsMap(props, readerContext);
      keyToNewRecords.entrySet().forEach(kv -> {
        HoodieRecord hoodieRecord = kv.getValue();
        BufferedRecord<T> bufferedRecord = (BufferedRecord<T>) BufferedRecord.forRecordWithContext(hoodieRecord, readerContext.getSchemaHandler().getTableSchema(),
            readerContext.getRecordContext(), props);
        records.put(hoodieRecord.getRecordKey(), bufferedRecord);
      });
      RecordsBasedRecordBuffer reusableBuffer = new RecordsBasedRecordBuffer(
          readerContext, hoodieTableMetaClient, readerContext.getMergeMode(), partialUpdateMode, props, orderingFieldNames, updateProcessor, records);
      return Pair.of(reusableBuffer, Collections.emptyList());
    } catch (IOException e) {
      throw new HoodieIOException("Failed to create record buffer");
    }
  }

  protected ExternalSpillableMap<Serializable, BufferedRecord<T>> initializeRecordsMap(TypedProperties props,
                                                                                       HoodieReaderContext<T> readerContext) throws IOException {
    String spillableMapBasePath = props.getString(SPILLABLE_MAP_BASE_PATH.key(), FileIOUtils.getDefaultSpillableMapBasePath());
    long maxMemorySizeInBytes = props.getLong(MAX_MEMORY_FOR_MERGE.key(), MAX_MEMORY_FOR_MERGE.defaultValue());
    ExternalSpillableMap.DiskMapType diskMapType = ExternalSpillableMap.DiskMapType.valueOf(props.getString(SPILLABLE_DISK_MAP_TYPE.key(),
        SPILLABLE_DISK_MAP_TYPE.defaultValue().name()).toUpperCase(Locale.ROOT));
    boolean isBitCaskDiskMapCompressionEnabled = props.getBoolean(DISK_MAP_BITCASK_COMPRESSION_ENABLED.key(),
        DISK_MAP_BITCASK_COMPRESSION_ENABLED.defaultValue());
    return new ExternalSpillableMap<>(maxMemorySizeInBytes, spillableMapBasePath, new DefaultSizeEstimator<>(),
        readerContext.getRecordSizeEstimator(), diskMapType, readerContext.getRecordSerializer(), isBitCaskDiskMapCompressionEnabled, getClass().getSimpleName());
  }
}
