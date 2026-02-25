/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.spark.sql.execution.datasources.parquet

import org.apache.hudi.HoodieTableSchema
import org.apache.hudi.common.model.HoodieFileFormat
import org.apache.hudi.internal.schema.InternalSchema

import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}
import org.junit.jupiter.api.Assertions.{assertFalse, assertTrue}
import org.junit.jupiter.api.Test

class TestHoodieFileGroupReaderBasedFileFormat {

  private val dummyTableSchema: HoodieTableSchema = {
    val structType = new StructType(Array[StructField](
      StructField("id", DataTypes.IntegerType, nullable = false, Metadata.empty)))
    val avroSchemaStr =
      """{"type": "record", "name": "test", "fields": [{"name": "id", "type": "int"}]}"""
    HoodieTableSchema(structType, avroSchemaStr, Option.empty[InternalSchema])
  }

  private def createFormat(isMOR: Boolean = false,
                           isBootstrap: Boolean = false,
                           isIncremental: Boolean = false,
                           isMultipleBaseFileFormatsEnabled: Boolean = false,
                           hoodieFileFormat: HoodieFileFormat = HoodieFileFormat.PARQUET
                          ): HoodieFileGroupReaderBasedFileFormat = {
    new HoodieFileGroupReaderBasedFileFormat(
      tablePath = "/tmp/test_table",
      tableSchema = dummyTableSchema,
      tableName = "test_table",
      queryTimestamp = "20240101000000",
      mandatoryFields = Seq.empty,
      isMOR = isMOR,
      isBootstrap = isBootstrap,
      isIncremental = isIncremental,
      validCommits = "",
      shouldUseRecordPosition = false,
      requiredFilters = Seq.empty[Filter],
      isMultipleBaseFileFormatsEnabled = isMultipleBaseFileFormatsEnabled,
      hoodieFileFormat = hoodieFileFormat)
  }

  @Test
  def testCanOffloadCowSnapshot(): Unit = {
    val format = createFormat()
    assertTrue(format.canOffloadToNativeParquetReader,
      "COW snapshot with PARQUET format should be offloadable to native parquet reader")
  }

  @Test
  def testCannotOffloadMor(): Unit = {
    val format = createFormat(isMOR = true)
    assertFalse(format.canOffloadToNativeParquetReader,
      "MOR table should not be offloadable to native parquet reader")
  }

  @Test
  def testCannotOffloadBootstrap(): Unit = {
    val format = createFormat(isBootstrap = true)
    assertFalse(format.canOffloadToNativeParquetReader,
      "Bootstrap table should not be offloadable to native parquet reader")
  }

  @Test
  def testCannotOffloadIncremental(): Unit = {
    val format = createFormat(isIncremental = true)
    assertFalse(format.canOffloadToNativeParquetReader,
      "Incremental query should not be offloadable to native parquet reader")
  }

  @Test
  def testCannotOffloadMultipleBaseFileFormats(): Unit = {
    val format = createFormat(isMultipleBaseFileFormatsEnabled = true)
    assertFalse(format.canOffloadToNativeParquetReader,
      "Multiple base file formats should not be offloadable to native parquet reader")
  }

  @Test
  def testCannotOffloadOrcFormat(): Unit = {
    val format = createFormat(hoodieFileFormat = HoodieFileFormat.ORC)
    assertFalse(format.canOffloadToNativeParquetReader,
      "ORC format should not be offloadable to native parquet reader")
  }
}
