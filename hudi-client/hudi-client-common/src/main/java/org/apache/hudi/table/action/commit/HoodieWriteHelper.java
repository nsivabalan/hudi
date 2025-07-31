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

package org.apache.hudi.table.action.commit;

import org.apache.hudi.avro.HoodieAvroReaderContext;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.SerializableSchema;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.engine.RecordContext;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.table.read.BufferedRecordMerger;
import org.apache.hudi.common.table.read.FileGroupReaderSchemaHandler;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.HoodieTable;

import org.apache.avro.Schema;

import java.io.IOException;
import java.util.List;

public class HoodieWriteHelper<T, R> extends BaseWriteHelper<T, HoodieData<HoodieRecord<T>>,
    HoodieData<HoodieKey>, HoodieData<WriteStatus>, R> {

  private HoodieWriteHelper() {
    super(HoodieData::deduceNumPartitions);
  }

  private static class WriteHelperHolder {
    private static final HoodieWriteHelper HOODIE_WRITE_HELPER = new HoodieWriteHelper<>();
  }

  public static HoodieWriteHelper newInstance() {
    return WriteHelperHolder.HOODIE_WRITE_HELPER;
  }

  @Override
  protected HoodieData<HoodieRecord<T>> tag(HoodieData<HoodieRecord<T>> dedupedRecords, HoodieEngineContext context,
                                            HoodieTable<T, HoodieData<HoodieRecord<T>>, HoodieData<HoodieKey>, HoodieData<WriteStatus>> table) {
    return table.getIndex().tagLocation(dedupedRecords, context, table);
  }

  @Override
  public HoodieData<HoodieRecord<T>> deduplicateRecords(HoodieData<HoodieRecord<T>> records,
                                                        HoodieIndex<?, ?> index,
                                                        int parallelism,
                                                        String schemaStr,
                                                        TypedProperties props,
                                                        BufferedRecordMerger<T> recordMerger,
                                                        HoodieReaderContext<T> readerContext,
                                                        List<String> orderingFieldNames) {
    boolean isIndexingGlobal = index.isGlobal();
    final SerializableSchema schema = new SerializableSchema(schemaStr);
    RecordContext recordContext = readerContext.getRecordContext();
    Schema writerSchema = new Schema.Parser().parse(schemaStr);
    Pair<Option<Pair<String, String>>, Boolean> deleteConfigs = FileGroupReaderSchemaHandler.getDeleteConfigs(props, writerSchema);
    Option<Pair<String, String>> customDeleteMarkerKeyValue = deleteConfigs.getLeft();
    boolean hasBuiltInDelete = deleteConfigs.getRight();
    int hoodieOperationPos = FileGroupReaderSchemaHandler.getHoodieOperationPos(writerSchema);
    boolean isAvroReaderContext = readerContext instanceof HoodieAvroReaderContext;
    return records.mapToPair(record -> {
      HoodieKey hoodieKey = record.getKey();
      // If index used is global, then records are expected to differ in their partitionPath
      Object key = isIndexingGlobal ? hoodieKey.getRecordKey() : hoodieKey;
      // NOTE: PLEASE READ CAREFULLY BEFORE CHANGING
      //       Here we have to make a copy of the incoming record, since it might be holding
      //       an instance of [[InternalRow]] pointing into shared, mutable buffer
      return Pair.of(key, record.copy());
    }).reduceByKey((rec1, rec2) -> {
      HoodieRecord<T> reducedRecord;
      try {
        HoodieRecord newRecord = rec1;
        HoodieRecord oldRecord = rec2;
        newRecord = rec1.toIndexedRecord(schema.get(), props).get();
        oldRecord = rec2.toIndexedRecord(schema.get(), props).get();
        /*if (isAvroReaderContext) {
          // We need to convert HoodieAvroRecord to HoodieAvroIndexedRecord in order to use the reader context
          newRecord = rec1.toIndexedRecord(schema.get(), props).get();
          oldRecord = rec2.toIndexedRecord(schema.get(), props).get();
        }*/
        // NOTE: The order of rec1 and rec2 is uncertain within "reduceByKey".
        Option<BufferedRecord<T>> merged = merge(
            newRecord, oldRecord, schema.get(), schema.get(), recordContext, orderingFieldNames, recordMerger,
            hasBuiltInDelete, customDeleteMarkerKeyValue, hoodieOperationPos, props);
        // NOTE: For merge mode based merging, it returns non-null.
        //       For mergers / payloads based merging, it may return null.
        reducedRecord = recordContext.constructHoodieRecord(merged.get());
        // convert back to HoodieRecord of type payload. punt this for now.
        // lets return the HoodieRecord of type IndexedRecord for now back to driver.
      } catch (IOException e) {
        throw new HoodieException(String.format("Error to merge two records, %s, %s", rec1, rec2), e);
      }
      return reducedRecord.newInstance(rec1.getKey(), reducedRecord.getOperation());
    }, parallelism).map(Pair::getRight);
  }
}
