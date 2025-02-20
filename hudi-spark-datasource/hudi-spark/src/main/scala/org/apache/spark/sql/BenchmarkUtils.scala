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

package org.apache.spark.sql

import org.apache.hudi.common.model.{HoodieKey, HoodieRecord, HoodieRecordLocation, HoodieSparkRecord}
import org.apache.hudi.common.testutils.HoodieTestDataGenerator
import org.apache.hudi.common.testutils.RawTripTestPayload.recordsToStrings
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.sql.types.StructType

import scala.jdk.CollectionConverters.asScalaBufferConverter

object BenchmarkUtils {

  def generateInputRecords(dataGen: HoodieTestDataGenerator, totalRecords: Integer): java.util.List[HoodieRecord[_]] = {
    dataGen.generateInserts("000", totalRecords).asInstanceOf[java.util.List[HoodieRecord[_]]]
  }

  def generateInputDf(inputRecords: java.util.List[HoodieRecord[_]], spark: SparkSession): DataFrame = {
    val records = recordsToStrings(inputRecords).asScala.toList
    spark.read.json(spark.sparkContext.parallelize(records, 2))
  }

  def convertToDatasetHoodieRecord(inputDF: DataFrame, structType: StructType): JavaRDD[HoodieSparkRecord] = {
    inputDF.queryExecution.toRdd.mapPartitions { it =>

      it.map { sourceRow =>
        val internalRowCopy = sourceRow.copy()
        val (key: HoodieKey, recordLocation: Option[HoodieRecordLocation]) = (new HoodieKey(internalRowCopy.getString(1), internalRowCopy.getString(15)), Option.empty)
        //HoodieCreateRecordUtils.getHoodieKeyAndMayBeLocationFromSparkRecord(sparkKeyGenerator, internalRowCopy, structType, false, false)
        val hoodieSparkRecord = new HoodieSparkRecord(key, internalRowCopy, structType, false)
        if (recordLocation.isDefined) hoodieSparkRecord.setCurrentLocation(recordLocation.get)
        hoodieSparkRecord
      }
    }.toJavaRDD().asInstanceOf[JavaRDD[HoodieSparkRecord]]
  }
}
