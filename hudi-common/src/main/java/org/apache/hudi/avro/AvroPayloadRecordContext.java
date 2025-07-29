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

import org.apache.hudi.common.engine.RecordContext;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.util.AvroJavaTypeConverter;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.SpillableMapUtils;
import org.apache.hudi.exception.HoodieIOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

public class AvroPayloadRecordContext extends RecordContext<HoodieRecordPayload> {

  private final String payloadClass;

  public AvroPayloadRecordContext(HoodieTableConfig tableConfig) {
    super(tableConfig);
    this.payloadClass = tableConfig.getPayloadClass();
    this.typeConverter = new AvroJavaTypeConverter();
  }

  @Override
  public Object getValue(HoodieRecordPayload record, Schema schema, String fieldName) {
    try {
      Option<IndexedRecord> recordValue = record.getInsertValue(schema, new Properties());
      if (recordValue.isPresent()) {
        return getFieldValueFromIndexedRecord(recordValue.get(), fieldName);
      } else {
        return null;
      }
    } catch (IOException e) {
      throw new HoodieIOException("Failed to deser", e);
    }
  }

  @Override
  public String getMetaFieldValue(HoodieRecordPayload record, int pos) {
    throw new HoodieIOException("Unsupported operation ");
    /*try {
      Option<IndexedRecord> recordValue = record.getInsertValue(schema, new Properties());
      if (recordValue.isPresent()) {
        return recordValue.get().get(pos).toString();
      } else {
        return null;
      }
    } catch (IOException e) {
      throw new HoodieIOException("Failed to deser", e);
    }*/
  }

  @Override
  public HoodieRecord<HoodieRecordPayload> constructHoodieRecord(BufferedRecord<HoodieRecordPayload> bufferedRecord) {
    if (bufferedRecord.isDelete()) {
      return SpillableMapUtils.generateEmptyPayload(
          bufferedRecord.getRecordKey(),
          partitionPath,
          bufferedRecord.getOrderingValue(),
          payloadClass);
    }
    HoodieKey hoodieKey = new HoodieKey(bufferedRecord.getRecordKey(), partitionPath);
    return new HoodieAvroRecord(hoodieKey, bufferedRecord.getRecord());
  }

  @Override
  public HoodieRecordPayload mergeWithEngineRecord(Schema schema,
                                                   Map<Integer, Object> updateValues,
                                                   BufferedRecord<HoodieRecordPayload> baseRecord) {
    HoodieRecordPayload engineRecord = baseRecord.getRecord();
    try {
      Option<IndexedRecord> recordValue = engineRecord.getInsertValue(schema, new Properties());
      if (recordValue.isPresent()) {
        GenericRecord genericRecord = (GenericRecord) recordValue.get();
        for (Map.Entry<Integer, Object> value : updateValues.entrySet()) {
          genericRecord.put(value.getKey(), value.getValue());
        }
        // construct payload.
        return createPayload(payloadClass, genericRecord);
      } else {
        return SpillableMapUtils.generateEmptyPayload(
            baseRecord.getRecordKey(),
            partitionPath,
            baseRecord.getOrderingValue(),
            payloadClass);
      }
    } catch (IOException e) {
      throw new HoodieIOException("Failed to deser", e);
    }
  }

  @Override
  public HoodieRecordPayload convertAvroRecord(IndexedRecord record) {
    try {
      // construct payload.
      return createPayload(payloadClass, (GenericRecord) record);
    } catch (IOException e) {
      throw new HoodieIOException("Failed to deser", e);
    }
  }

  // need to add Props argument.
  @Override
  public GenericRecord convertToAvroRecord(HoodieRecordPayload record, Schema schema) {
    try {
      return (GenericRecord) record.getInsertValue(schema, new Properties()).get();
    } catch (IOException e) {
      throw new HoodieIOException("Failed to deser", e);
    }
  }

  @Override
  public HoodieRecordPayload getDeleteRow(HoodieRecordPayload record, String recordKey) {
    throw new UnsupportedOperationException("Not supported for " + this.getClass().getSimpleName());
  }

  public static Object getFieldValueFromIndexedRecord(
      IndexedRecord record,
      String fieldName) {
    Schema currentSchema = record.getSchema();
    IndexedRecord currentRecord = record;
    String[] path = fieldName.split("\\.");
    for (int i = 0; i < path.length; i++) {
      if (currentSchema.isUnion()) {
        currentSchema = AvroSchemaUtils.resolveNullableSchema(currentSchema);
      }
      Schema.Field field = currentSchema.getField(path[i]);
      if (field == null) {
        return null;
      }
      Object value = currentRecord.get(field.pos());
      if (i == path.length - 1) {
        return value;
      }
      currentSchema = field.schema();
      currentRecord = (IndexedRecord) value;
    }
    return null;
  }

  /**
   * Create a payload class via reflection, do not ordering/precombine value.
   */
  public static HoodieRecordPayload createPayload(String payloadClass, GenericRecord record)
      throws IOException {
    try {
      return (HoodieRecordPayload) ReflectionUtils.loadClass(payloadClass,
          new Class<?>[] {Option.class}, Option.of(record));
    } catch (Throwable e) {
      throw new IOException("Could not create payload for class: " + payloadClass, e);
    }
  }
}
