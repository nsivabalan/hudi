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

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnWriteStore;
import org.apache.parquet.column.ColumnWriter;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Factory + leaf implementations of {@link ParquetValueWriter} for the lake-loader bulk-insert
 * row-writer fast path.
 *
 * <p>Each leaf writer holds a direct reference to its parquet {@link ColumnWriter} and knows
 * how to pull its typed value out of a {@link SpecializedGetters} (a Spark
 * {@link InternalRow} or {@link org.apache.spark.sql.catalyst.util.ArrayData}). On the hot
 * path the struct writer simply dispatches {@code child.writeColumn(getters, ordinal, rl)}
 * — there is no per-field type switch, no boxing through {@code Object}, and no per-record
 * BitSet bookkeeping.
 *
 * <p>For v1 we support the primitive types lake-loader's default schema uses: boolean, int,
 * long, float, double, string (via {@code Binary.fromReusedByteArray}), and binary.
 * Unsupported types cause {@link #buildStruct} to throw {@link UnsupportedOperationException}
 * so the factory can fall back to the existing WriteSupport-based writer.
 */
public final class ParquetValueWriters {

  private ParquetValueWriters() {
  }

  /**
   * Build the root {@link InternalRowStructWriter} for a top-level Spark schema. The struct
   * shape is validated up-front against the parquet schema so the hot path stays branch-free.
   *
   * <p>The returned writer is unbound — callers must invoke
   * {@link InternalRowStructWriter#setColumnStore(ColumnWriteStore)} before writing rows
   * and again after every row-group rollover.
   *
   * @throws UnsupportedOperationException if any field uses a type not yet handled by this
   *     fast path. The caller should fall back to the legacy WriteSupport writer.
   */
  public static InternalRowStructWriter buildStruct(StructType structType,
                                                    MessageType parquetSchema) {
    StructField[] fields = structType.fields();
    if (fields.length != parquetSchema.getFieldCount()) {
      throw new IllegalStateException("StructType and MessageType field counts differ: "
          + fields.length + " vs " + parquetSchema.getFieldCount());
    }
    LeafWriter[] children = new LeafWriter[fields.length];
    for (int i = 0; i < fields.length; i++) {
      // Resolve the leaf column for this top-level field. For primitives (lake-loader's
      // fast path), each field corresponds to exactly one parquet column reachable at
      // path = [fieldName].
      String fieldName = parquetSchema.getType(i).getName();
      ColumnDescriptor desc = parquetSchema.getColumnDescription(new String[] {fieldName});
      int maxDl = desc.getMaxDefinitionLevel();   // 1 for nullable top-level, 0 for required
      children[i] = buildLeaf(fields[i].dataType(), desc, maxDl);
    }
    return new InternalRowStructWriter(fields, children);
  }

  private static LeafWriter buildLeaf(DataType dataType, ColumnDescriptor desc, int maxDl) {
    if (dataType instanceof IntegerType) {
      return new IntWriter(desc, maxDl);
    } else if (dataType instanceof LongType) {
      return new LongWriter(desc, maxDl);
    } else if (dataType instanceof FloatType) {
      return new FloatWriter(desc, maxDl);
    } else if (dataType == DataTypes.DoubleType) {
      return new DoubleWriter(desc, maxDl);
    } else if (dataType == DataTypes.BooleanType) {
      return new BooleanWriter(desc, maxDl);
    } else if (dataType instanceof StringType) {
      return new UTF8StringWriter(desc, maxDl);
    } else if (dataType == DataTypes.BinaryType) {
      return new BinaryWriter(desc, maxDl);
    }
    throw new UnsupportedOperationException(
        "Direct ColumnWriteStore writer does not yet support DataType: " + dataType
            + " — fall back to the WriteSupport-based writer.");
  }

  /**
   * Root writer over a top-level {@link InternalRow}. The hot loop is intentionally tiny —
   * one null check and one virtual call per field — and free of any per-record allocations
   * or BitSet bookkeeping.
   */
  public static final class InternalRowStructWriter implements ParquetValueWriter<InternalRow> {
    private final StructField[] fields;     // retained for diagnostics only
    private final LeafWriter[] children;

    InternalRowStructWriter(StructField[] fields, LeafWriter[] children) {
      this.fields = fields;
      this.children = children;
    }

    @Override
    public void write(int rl, InternalRow row) {
      LeafWriter[] childWriters = children;
      for (int i = 0; i < childWriters.length; i++) {
        LeafWriter child = childWriters[i];
        if (row.isNullAt(i)) {
          child.writeNull(rl);
        } else {
          child.writeColumn(row, i, rl);
        }
      }
    }

    @Override
    public void setColumnStore(ColumnWriteStore columnStore) {
      for (LeafWriter child : children) {
        child.setColumnStore(columnStore);
      }
    }
  }

  /**
   * Leaf writer for a single parquet column. Subclasses implement {@link #writeColumn} to
   * pull their typed value from the {@link SpecializedGetters} and emit a single
   * {@code columnWriter.write(value, rl, maxDl)} call.
   */
  abstract static class LeafWriter implements ParquetValueWriter<Object> {
    final ColumnDescriptor desc;
    final int maxDl;
    ColumnWriter column;

    LeafWriter(ColumnDescriptor desc, int maxDl) {
      this.desc = desc;
      this.maxDl = maxDl;
    }

    @Override
    public final void setColumnStore(ColumnWriteStore columnStore) {
      this.column = columnStore.getColumnWriter(desc);
    }

    /**
     * Default {@link ParquetValueWriter#write(int, Object)} is unused — the struct writer
     * dispatches through {@link #writeColumn} which avoids the {@code Object} box.
     */
    @Override
    public final void write(int rl, Object value) {
      throw new UnsupportedOperationException(
          "LeafWriter.write(int, Object) is not used on the hot path — call writeColumn.");
    }

    /** Pull the typed value out of {@code getters[ordinal]} and write it. */
    abstract void writeColumn(SpecializedGetters getters, int ordinal, int rl);

    final void writeNull(int rl) {
      column.writeNull(rl, maxDl - 1);
    }
  }

  static final class IntWriter extends LeafWriter {
    IntWriter(ColumnDescriptor desc, int maxDl) {
      super(desc, maxDl);
    }

    @Override
    void writeColumn(SpecializedGetters getters, int ordinal, int rl) {
      column.write(getters.getInt(ordinal), rl, maxDl);
    }
  }

  static final class LongWriter extends LeafWriter {
    LongWriter(ColumnDescriptor desc, int maxDl) {
      super(desc, maxDl);
    }

    @Override
    void writeColumn(SpecializedGetters getters, int ordinal, int rl) {
      column.write(getters.getLong(ordinal), rl, maxDl);
    }
  }

  static final class FloatWriter extends LeafWriter {
    FloatWriter(ColumnDescriptor desc, int maxDl) {
      super(desc, maxDl);
    }

    @Override
    void writeColumn(SpecializedGetters getters, int ordinal, int rl) {
      column.write(getters.getFloat(ordinal), rl, maxDl);
    }
  }

  static final class DoubleWriter extends LeafWriter {
    DoubleWriter(ColumnDescriptor desc, int maxDl) {
      super(desc, maxDl);
    }

    @Override
    void writeColumn(SpecializedGetters getters, int ordinal, int rl) {
      column.write(getters.getDouble(ordinal), rl, maxDl);
    }
  }

  static final class BooleanWriter extends LeafWriter {
    BooleanWriter(ColumnDescriptor desc, int maxDl) {
      super(desc, maxDl);
    }

    @Override
    void writeColumn(SpecializedGetters getters, int ordinal, int rl) {
      column.write(getters.getBoolean(ordinal), rl, maxDl);
    }
  }

  static final class UTF8StringWriter extends LeafWriter {
    UTF8StringWriter(ColumnDescriptor desc, int maxDl) {
      super(desc, maxDl);
    }

    @Override
    void writeColumn(SpecializedGetters getters, int ordinal, int rl) {
      column.write(Binary.fromReusedByteArray(getters.getUTF8String(ordinal).getBytes()), rl, maxDl);
    }
  }

  static final class BinaryWriter extends LeafWriter {
    BinaryWriter(ColumnDescriptor desc, int maxDl) {
      super(desc, maxDl);
    }

    @Override
    void writeColumn(SpecializedGetters getters, int ordinal, int rl) {
      column.write(Binary.fromReusedByteArray(getters.getBinary(ordinal)), rl, maxDl);
    }
  }
}
