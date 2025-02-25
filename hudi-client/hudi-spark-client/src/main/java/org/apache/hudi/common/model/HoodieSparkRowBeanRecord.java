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

import org.apache.spark.sql.Row;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class HoodieSparkRowBeanRecord implements Serializable {

  /**
   * Identifies the record across the table.
   */
  private HoodieKey key;

  /**
   * Actual payload of the record.
   */
  private Row data;

  /**
   * Current location of record on storage. Filled in by looking up index
   */
  private HoodieRecordLocation currentLocation;

  /**
   * New location of record on storage, after written.
   */
  private HoodieRecordLocation newLocation;

  /**
   * If set, not update index after written.
   */
  private boolean ignoreIndexUpdate;

  /**
   * Indicates whether the object is sealed.
   */
  private boolean sealed;

  /**
   * The cdc operation.
   */
  private HoodieOperation operation;

  /**
   * The metaData of the record.
   */
  private Map<String, String> metaData = new HashMap<>();

  /**
   * Record copy operation to avoid double copying. InternalRow do not need to copy twice.
   */
  private boolean copy;

  public HoodieSparkRowBeanRecord() {

  }

  public HoodieKey getKey() {
    return key;
  }

  public void setKey(HoodieKey key) {
    this.key = key;
  }

  public Row getData() {
    return data;
  }

  public void setData(Row data) {
    this.data = data;
  }

  public HoodieRecordLocation getCurrentLocation() {
    return currentLocation;
  }

  public void setCurrentLocation(HoodieRecordLocation currentLocation) {
    this.currentLocation = currentLocation;
  }

  public HoodieRecordLocation getNewLocation() {
    return newLocation;
  }

  public void setNewLocation(HoodieRecordLocation newLocation) {
    this.newLocation = newLocation;
  }

  public boolean isIgnoreIndexUpdate() {
    return ignoreIndexUpdate;
  }

  public void setIgnoreIndexUpdate(boolean ignoreIndexUpdate) {
    this.ignoreIndexUpdate = ignoreIndexUpdate;
  }

  public boolean isSealed() {
    return sealed;
  }

  public void setSealed(boolean sealed) {
    this.sealed = sealed;
  }

  public HoodieOperation getOperation() {
    return operation;
  }

  public void setOperation(HoodieOperation operation) {
    this.operation = operation;
  }

  public Map<String, String> getMetaData() {
    return metaData;
  }

  public void setMetaData(Map<String, String> metaData) {
    this.metaData = metaData;
  }

  public boolean isCopy() {
    return copy;
  }

  public void setCopy(boolean copy) {
    this.copy = copy;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof HoodieSparkRowBeanRecord)) {
      return false;
    }
    HoodieSparkRowBeanRecord that = (HoodieSparkRowBeanRecord) o;
    return isIgnoreIndexUpdate() == that.isIgnoreIndexUpdate() && isSealed() == that.isSealed() && isCopy() == that.isCopy() && getKey().equals(that.getKey()) && getData().equals(that.getData()) &&
        Objects.equals(getCurrentLocation(), that.getCurrentLocation()) && Objects.equals(getNewLocation(), that.getNewLocation()) && getOperation() == that.getOperation() &&
        Objects.equals(getMetaData(), that.getMetaData());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getKey(), getData(), getCurrentLocation(), getNewLocation(), isIgnoreIndexUpdate(), isSealed(), getOperation(), getMetaData(), isCopy());
  }
}
