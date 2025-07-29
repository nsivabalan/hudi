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

import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.SerializableSchema;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.engine.RecordContext;
import org.apache.hudi.common.function.SerializableFunctionUnchecked;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.table.read.BufferedRecordMerger;
import org.apache.hudi.common.table.read.BufferedRecordMergerFactory;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import org.apache.avro.Schema;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.hudi.common.model.HoodieRecord.HOODIE_IS_DELETED_FIELD;

public abstract class BaseWriteHelper<T, I, K, O, R> extends ParallelismHelper<I> {

  protected BaseWriteHelper(SerializableFunctionUnchecked<I, Integer> partitionNumberExtractor) {
    super(partitionNumberExtractor);
  }

  public HoodieWriteMetadata<O> write(String instantTime,
                                      I inputRecords,
                                      HoodieEngineContext context,
                                      HoodieTable<T, I, K, O> table,
                                      boolean shouldCombine,
                                      int configuredShuffleParallelism,
                                      BaseCommitActionExecutor<T, I, K, O, R> executor,
                                      WriteOperationType operationType) {
    try {
      HoodieTimer sourceReadAndIndexTimer = HoodieTimer.start();
      // De-dupe/merge if needed
      I dedupedRecords =
          combineOnCondition(shouldCombine, inputRecords, configuredShuffleParallelism, table);

      I taggedRecords = dedupedRecords;
      if (table.getIndex().requiresTagging(operationType)) {
        // perform index loop up to get existing location of records
        context.setJobStatus(this.getClass().getSimpleName(), "Tagging: " + table.getConfig().getTableName());
        taggedRecords = tag(dedupedRecords, context, table);
      }

      HoodieWriteMetadata<O> result = executor.execute(taggedRecords, Option.of(sourceReadAndIndexTimer));
      return result;
    } catch (Throwable e) {
      if (e instanceof HoodieUpsertException) {
        throw (HoodieUpsertException) e;
      }
      throw new HoodieUpsertException("Failed to upsert for commit time " + instantTime, e);
    }
  }

  protected abstract I tag(
      I dedupedRecords, HoodieEngineContext context, HoodieTable<T, I, K, O> table);

  public I combineOnCondition(
      boolean condition, I records, int configuredParallelism, HoodieTable<T, I, K, O> table) {
    int targetParallelism = deduceShuffleParallelism(records, configuredParallelism);
    return condition ? deduplicateRecords(records, table, targetParallelism) : records;
  }

  /**
   * Deduplicate Hoodie records, using the given deduplication function.
   *
   * @param records     hoodieRecords to deduplicate
   * @param parallelism parallelism or partitions to be used while reducing/deduplicating
   * @return Collection of HoodieRecord already be deduplicated
   */
  public I deduplicateRecords(I records, HoodieTable<T, I, K, O> table, int parallelism) {
    HoodieReaderContext<T> readerContext =
        (HoodieReaderContext<T>) table.getContext().<T>getReaderContextFactoryDuringWrite(table.getMetaClient(), table.getConfig().getRecordMerger().getRecordType())
            .getContext();
    List<String> orderingFieldNames = getOrderingFieldName(readerContext, table.getConfig().getProps(), table.getMetaClient());
    BufferedRecordMerger<T> recordMerger = BufferedRecordMergerFactory.create(
        readerContext,
        table.getConfig().getRecordMergeMode(),
        false,
        Option.ofNullable(table.getConfig().getRecordMerger()),
        orderingFieldNames,
        Option.ofNullable(table.getConfig().getPayloadClass()),
        new SerializableSchema(table.getConfig().getSchema()).get(),
        table.getConfig().getProps(),
        table.getMetaClient().getTableConfig().getPartialUpdateMode());
    // Due to new records we cant use meta fields for record key extraction
    readerContext.getRecordContext().updateRecordKeyExtractor(table.getMetaClient().getTableConfig(), false);
    return deduplicateRecords(
        records,
        table.getIndex(),
        parallelism,
        table.getConfig().getSchema(),
        table.getConfig().getProps(),
        recordMerger,
        readerContext,
        orderingFieldNames);
  }

  public abstract I deduplicateRecords(I records,
                                       HoodieIndex<?, ?> index,
                                       int parallelism,
                                       String schema,
                                       TypedProperties props,
                                       BufferedRecordMerger<T> merger,
                                       HoodieReaderContext<T> readerContext,
                                       List<String> orderingFieldNames);

  public static List<String> getOrderingFieldName(HoodieReaderContext readerContext,
                                                  TypedProperties props,
                                                  HoodieTableMetaClient metaClient) {
    return readerContext.getMergeMode() == RecordMergeMode.COMMIT_TIME_ORDERING
        ? Collections.emptyList()
        : Option.ofNullable(ConfigUtils.getOrderingFields(props)).map(Arrays::asList).orElse(metaClient.getTableConfig().getPreCombineFields());
  }

  /**
   * Check if the value of column "_hoodie_is_deleted" is true.
   */
  public static <T> boolean isBuiltInDeleteRecord(T record,
                                                  RecordContext<T> recordContext,
                                                  Schema schema, Option<Pair<String, String>> customDeleteMarkerKeyValue) {
    if (!customDeleteMarkerKeyValue.isPresent()) {
      return false;
    }
    Object columnValue = recordContext.getValue(record, schema, HOODIE_IS_DELETED_FIELD);
    return columnValue != null && recordContext.getTypeConverter().castToBoolean(columnValue);
  }

  /**
   * Check if a record is a DELETE marked by the '_hoodie_operation' field.
   */
  public static <T> boolean isDeleteHoodieOperation(T record,
                                                    RecordContext<T> recordContext,
                                                    int hoodieOperationPos) {
    if (hoodieOperationPos < 0) {
      return false;
    }
    String hoodieOperation = recordContext.getMetaFieldValue(record, hoodieOperationPos);
    return hoodieOperation != null && HoodieOperation.isDeleteRecord(hoodieOperation);
  }

  /**
   * Check if a record is a DELETE marked by a custom delete marker.
   */
  public static <T> boolean isCustomDeleteRecord(T record,
                                                 RecordContext<T> recordContext,
                                                 Schema schema,
                                                 boolean hasBuiltInDelete,
                                                 Option<Pair<String, String>> customDeleteMarkerKeyValue) {
    if (!hasBuiltInDelete || customDeleteMarkerKeyValue.isEmpty()) {
      return false;
    }
    Pair<String, String> markerKeyValue = customDeleteMarkerKeyValue.get();
    Object deleteMarkerValue =
        recordContext.getValue(record, schema, markerKeyValue.getLeft());
    return deleteMarkerValue != null
        && markerKeyValue.getRight().equals(deleteMarkerValue.toString());
  }

  // to do: pass in properties from higher layer to fetch the data.
  public static <T> Option<BufferedRecord<T>> merge(HoodieRecord<T> newRecord,
                                                    HoodieRecord<T> oldRecord,
                                                    Schema newSchema,
                                                    Schema oldSchema,
                                                    RecordContext<T> recordContext,
                                                    List<String> orderingFieldNames,
                                                    BufferedRecordMerger<T> recordMerger,
                                                    boolean hasBuiltInDelete,
                                                    Option<Pair<String, String>> customDeleteMarkerKeyValue,
                                                    int hoodieOperationPos,
                                                    TypedProperties properties) throws IOException {
    // Construct new buffered record.
    boolean isDelete1 = isBuiltInDeleteRecord(newRecord.getData(), recordContext, newSchema, customDeleteMarkerKeyValue)
        || isCustomDeleteRecord(newRecord.getData(), recordContext, newSchema, hasBuiltInDelete, customDeleteMarkerKeyValue)
        || isDeleteHoodieOperation(newRecord.getData(), recordContext, hoodieOperationPos);
    BufferedRecord<T> bufferedRec1 = BufferedRecord.forRecordWithContext(
        newRecord.getData(), newSchema, recordContext, orderingFieldNames, isDelete1, Option.of(newRecord.getKey()), Option.of(newRecord.getOrderingValue(newSchema, properties)));
    // Construct old buffered record.
    boolean isDelete2 = isBuiltInDeleteRecord(oldRecord.getData(), recordContext, oldSchema, customDeleteMarkerKeyValue)
        || isCustomDeleteRecord(oldRecord.getData(), recordContext, oldSchema, hasBuiltInDelete, customDeleteMarkerKeyValue)
        || isDeleteHoodieOperation(oldRecord.getData(), recordContext, hoodieOperationPos);
    BufferedRecord<T> bufferedRec2 = BufferedRecord.forRecordWithContext(
        oldRecord.getData(), oldSchema, recordContext, orderingFieldNames, isDelete2, Option.of(oldRecord.getKey()), Option.of(oldRecord.getOrderingValue(oldSchema, properties)));
    // Run merge.
    return recordMerger.deltaMerge(bufferedRec1, bufferedRec2);
  }
}
