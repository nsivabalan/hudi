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
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType

import java.time.LocalDate
import java.util.UUID
import scala.jdk.CollectionConverters.asScalaBufferConverter
import scala.util.Random

object BenchmarkUtils {

  private val SEED: Long = 378294793957830L
  private val random = new Random(SEED)

  private val TEXT_VALUE: String = "abcdefghijklmnopqrstuvwxyz"

  def newRecord(round: Int,
                size: Int,
                partitionPaths: List[String]) = {
    val ts = System.currentTimeMillis()
    val key = s"${"%03d".format(round)}-${ts}-${randomUUID()}"

    Record(
      key           = key,
      partition     = partitionPaths(random.nextInt(partitionPaths.length)),
      ts            = ts,
      textField     = (0 until size/5/TEXT_VALUE.length).map(i => TEXT_VALUE).mkString("|"),
      decimalField  = random.nextFloat(),
      longField     = random.nextLong(),
      arrayField    = (0 until size/5).toArray,
      mapField      = (0 until size/2/40).map(_ => (randomUUID(), random.nextInt())).toMap,
      round
    )
  }

  private def randomUUID() : String =
    UUID.randomUUID().toString

  def genParallelRDD(spark: SparkSession, targetParallelism: Int, start: Long, end: Long): RDD[Long] = {
    val partitionSize = (end - start) / targetParallelism
    spark.sparkContext.parallelize(0 to targetParallelism, targetParallelism)
      .mapPartitions { it =>
        val partitionStart = it.next() * partitionSize
        (partitionStart to partitionStart + partitionSize).iterator
      }
  }

  def generateInput(spark: SparkSession, targetParallelism: Integer, numInserts: Long) : DataFrame = {
    val partitionPaths = genDateBasedPartitionValues(100)
    val insertsRDD = genParallelRDD(spark, targetParallelism, 0, numInserts)
      .map(_ => newRecord(0, 1024, partitionPaths))
    spark.createDataFrame(insertsRDD).toDF()
  }

  private def genDateBasedPartitionValues(targetPartitionsCount: Int): List[String] = {
    // This will generate an ordered sequence of dates in the format of "yyyy/mm/dd"
    // (where most recent one is the first element)
    List.fill(targetPartitionsCount)(LocalDate.now()).zipWithIndex
      .map(t => t._1.minusDays(targetPartitionsCount - t._2))
      .map(d => s"${d.getYear}/${"%02d".format(d.getMonthValue)}/${"%02d".format(d.getDayOfMonth)}")
      .reverse
  }

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
        val (key: HoodieKey, recordLocation: Option[HoodieRecordLocation]) = (new HoodieKey(internalRowCopy.getString(0), internalRowCopy.getString(1)), Option.empty)
        //HoodieCreateRecordUtils.getHoodieKeyAndMayBeLocationFromSparkRecord(sparkKeyGenerator, internalRowCopy, structType, false, false)
        val hoodieSparkRecord = new HoodieSparkRecord(key, internalRowCopy, structType, false)
        if (recordLocation.isDefined) hoodieSparkRecord.setCurrentLocation(recordLocation.get)
        hoodieSparkRecord
      }
    }.toJavaRDD().asInstanceOf[JavaRDD[HoodieSparkRecord]]
  }
}

case class Record(key: String,
                  partition: String,
                  ts: Long,
                  textField: String,
                  decimalField: Float,
                  longField: Long,
                  arrayField: Array[Int],
                  mapField: Map[String, Int],
                  round: Int)
