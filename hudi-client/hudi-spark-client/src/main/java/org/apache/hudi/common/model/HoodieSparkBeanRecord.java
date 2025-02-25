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

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

public class HoodieSparkBeanRecord implements Serializable {

  /**
   * Identifies the record across the table.
   */
  private HoodieKey key;

  private int unsafeRowSize;

  /**
   * Actual payload of the record.
   */
  private byte[] data;

  public HoodieSparkBeanRecord() {

  }

  public HoodieKey getKey() {
    return key;
  }

  public void setKey(HoodieKey key) {
    this.key = key;
  }

  public int getUnsafeRowSize() {
    return unsafeRowSize;
  }

  public void setUnsafeRowSize(int unsafeRowSize) {
    this.unsafeRowSize = unsafeRowSize;
  }

  public byte[] getData() {
    return data;
  }

  public void setData(byte[] data) {
    this.data = data;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof HoodieSparkBeanRecord)) {
      return false;
    }
    HoodieSparkBeanRecord that = (HoodieSparkBeanRecord) o;
    return getKey().equals(that.getKey()) && getData().equals(that.getData());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getKey(), getData());
  }
}
