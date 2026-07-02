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

import org.apache.parquet.column.ColumnWriteStore;

/**
 * A writer that converts a value of type {@code T} into Parquet column writes by issuing
 * calls directly against {@link org.apache.parquet.column.ColumnWriter}s obtained from a
 * {@link ColumnWriteStore}.
 *
 * <p>Modelled on Iceberg's {@code ParquetValueWriter}, this interface deliberately bypasses
 * parquet-mr's {@code MessageColumnIO}/{@code RecordConsumer} machinery: there is no
 * per-record {@code FieldsMarker} {@link java.util.BitSet} update, no field-ordinal lookup,
 * and no virtual dispatch through {@code MessageColumnIORecordConsumer}.
 *
 * <p>Each implementation either owns one or more leaf columns (in which case
 * {@link #setColumnStore(ColumnWriteStore)} resolves the column writer once at writer
 * construction time) or delegates to child writers.
 */
public interface ParquetValueWriter<T> {

  /**
   * Write {@code value} to the column(s) this writer owns at the given repetition level.
   *
   * <p>The struct writer at the top of the tree always passes {@code repetitionLevel=0}.
   * Array and map writers compute child repetition levels per element.
   */
  void write(int repetitionLevel, T value);

  /**
   * Resolve the underlying parquet {@link org.apache.parquet.column.ColumnWriter}s from the
   * given {@link ColumnWriteStore}. Must be called exactly once after construction and
   * exactly once after every row-group rollover (when the page/column store is rebuilt).
   *
   * <p>Composite writers (struct/array/map/option) propagate this call to their children.
   */
  void setColumnStore(ColumnWriteStore columnStore);
}
