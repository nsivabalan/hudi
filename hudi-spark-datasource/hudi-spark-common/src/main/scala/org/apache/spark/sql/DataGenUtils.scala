package org.apache.spark.sql

import org.apache.spark.rdd.RDD

import java.time.LocalDate
import java.util.UUID
import scala.util.Random

object DataGenUtils {

  private val SEED: Long = 378294793957830L
  private val random = new Random(SEED)

  private val TEXT_VALUE: String = "abcdefghijklmnopqrstuvwxyz"

  def newRecord(round: Int,
                size: Int,
                partitionPaths: List[String]) = {
    val ts = System.currentTimeMillis()
    val key = s"${"%03d".format(round)}-${ts}-${randomUUID()}"

    Record(
      key = key,
      partition = partitionPaths(random.nextInt(partitionPaths.length)),
      ts = ts,
      textField = (0 until size / 5 / TEXT_VALUE.length).map(i => TEXT_VALUE).mkString("|"),
      decimalField = random.nextFloat(),
      longField = random.nextLong(),
      arrayField = (0 until size / 5).toArray,
      mapField = (0 until size / 2 / 40).map(_ => (randomUUID(), random.nextInt())).toMap,
      round
    )
  }

  private def randomUUID(): String =
    UUID.randomUUID().toString

  def genParallelRDD(spark: SparkSession, targetParallelism: Int, start: Long, end: Long): RDD[Long] = {
    val partitionSize = (end - start) / targetParallelism
    spark.sparkContext.parallelize(0 to targetParallelism, targetParallelism)
      .mapPartitions { it =>
        val partitionStart = it.next() * partitionSize
        (partitionStart to partitionStart + partitionSize).iterator
      }
  }

  def generateInput(spark: SparkSession, targetParallelism: Integer, numInserts: Long): DataFrame = {
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

  /*def generateInputRecords(dataGen: HoodieTestDataGenerator, totalRecords: Integer): java.util.List[HoodieRecord[_]] = {
    dataGen.generateInserts("000", totalRecords).asInstanceOf[java.util.List[HoodieRecord[_]]]
  }

  def generateInputDf(inputRecords: java.util.List[HoodieRecord[_]], spark: SparkSession): DataFrame = {
    val records = recordsToStrings(inputRecords).asScala.toList
    spark.read.json(spark.sparkContext.parallelize(records, 2))
  }*/
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