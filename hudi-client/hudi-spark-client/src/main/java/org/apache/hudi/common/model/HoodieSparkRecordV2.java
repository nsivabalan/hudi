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

package org.apache.hudi.common.model;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.types.StructType;

public class HoodieSparkRecordV2 extends HoodieSparkRecord {
  public HoodieSparkRecordV2(UnsafeRow data) {
    super(data);
  }

  public HoodieSparkRecordV2(InternalRow data, StructType schema) {
    super(data, schema);
  }

  public HoodieSparkRecordV2(HoodieKey key, UnsafeRow data, boolean copy) {
    super(key, data, copy);
  }

  public HoodieSparkRecordV2(HoodieKey key, InternalRow data, StructType schema, boolean copy) {
    super(key, data, schema, copy);
  }

  public HoodieSparkRecordV2(HoodieKey key, InternalRow data, StructType schema, HoodieOperation operation, HoodieRecordLocation currentLocation,
                             HoodieRecordLocation newLocation, boolean copy) {
    super(key, data, schema, operation, currentLocation, newLocation, copy);
  }
}
