package org.apache.spark.sql

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Output
import org.apache.hudi.common.model.{HoodieKey, HoodieRecordLocation, HoodieSparkBeanRecord}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.sql.catalyst.expressions.UnsafeRow

object HoodieSparkBeanRecordUtils {

  def convertToDatasetHoodieBeanRecords(inputDF: DataFrame): JavaRDD[HoodieSparkBeanRecord] = {
    inputDF.queryExecution.toRdd.mapPartitions { it =>
      val kryo = new Kryo()
      it.map { sourceRow =>
        // key gen.
        val (key: HoodieKey, recordLocation: Option[HoodieRecordLocation]) = (new HoodieKey(sourceRow.getString(0), sourceRow.getString(1)), Option.empty)
        val hoodieSparkRecordBean = new HoodieSparkBeanRecord()
        hoodieSparkRecordBean.setKey(key)
        val unsafeRow = sourceRow.asInstanceOf[UnsafeRow]

        // serialize unsafeRow tp bytes
        val output = new Output(1024, -1)
        kryo.writeClassAndObject(output, unsafeRow);
        val serializedBytes = output.toBytes

        hoodieSparkRecordBean.setData(serializedBytes)
        hoodieSparkRecordBean
      }
    }.toJavaRDD().asInstanceOf[JavaRDD[HoodieSparkBeanRecord]]
  }

  /*def convertToDatasetHoodieRecord(inputDF: DataFrame, structType: StructType): JavaRDD[HoodieSparkRecord] = {
    inputDF.queryExecution.toRdd.mapPartitions { it =>

      it.map { sourceRow =>
        val (key: HoodieKey, recordLocation: Option[HoodieRecordLocation]) = (new HoodieKey(sourceRow.getString(0), sourceRow.getString(1)), Option.empty)
        //HoodieCreateRecordUtils.getHoodieKeyAndMayBeLocationFromSparkRecord(sparkKeyGenerator, internalRowCopy, structType, false, false)
        val hoodieSparkRecord = new HoodieSparkRecord(key, sourceRow, structType, false)
        if (recordLocation.isDefined) hoodieSparkRecord.setCurrentLocation(recordLocation.get)
        hoodieSparkRecord
      }
    }.toJavaRDD().asInstanceOf[JavaRDD[HoodieSparkRecord]]
  }

  def convertToDatasetHoodieAvroRecord(inputDF: DataFrame): JavaRDD[HoodieAvroRecord[_]] = {
    val avroRecords: RDD[GenericRecord] = HoodieSparkUtils.createRdd(inputDF, "sample_record_struct_name", "sample_record_name_space",
      None)
    avroRecords.map(avroRecord => {
      val hoodieKey = new HoodieKey((avroRecord.get("key").asInstanceOf[org.apache.avro.util.Utf8]).toString, (avroRecord.get("partition").asInstanceOf[org.apache.avro.util.Utf8]).toString)
      DataSourceUtils.createHoodieRecord(avroRecord, hoodieKey,
        "org.apache.hudi.common.model.OverwriteWithLatestAvroPayload", None)
    }).toJavaRDD().asInstanceOf[JavaRDD[HoodieAvroRecord[_]]]
  }*/
}
