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

package org.apache.hudi.benchmark;

import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.SparkAdapterSupport$;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieSparkRecord;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.BenchmarkUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.types.StructType;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.io.IOException;
import java.util.List;

@State(Scope.Benchmark)
public class DfBenchmarkExecutionPlan {

  @Param({"100000"})
  public int totalRecordsToTest;

  @Param("100")
  public int repartitionByNum;

  SparkSession spark;
  JavaSparkContext jssc;
  FileSystem fs;
  HoodieTestDataGenerator dataGen;
  int totalFields;
  Schema schema;
  Dataset<Row> inputDF;
  JavaRDD<Row> javaRddRecords;
  JavaRDD<HoodieSparkRecord> javaRDDHoodieRecords;
  Dataset<HoodieSparkRecord> datasetHoodieRecords;
  Dataset<HoodieSparkRecord> datasetHoodieRecordsKryo;
  ExpressionEncoder encoder;

  @Setup(Level.Iteration)
  public void doSetup() throws Exception {
    try {
      spark = SparkSession.builder().appName("Hoodie Write Benchmark")
          .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").master("local[2]").getOrCreate();
      jssc = new JavaSparkContext(spark.sparkContext());
      spark.sparkContext().setLogLevel("WARN");
      fs = FileSystem.get(jssc.hadoopConfiguration());
      dataGen = new HoodieTestDataGenerator();
      schema = HoodieTestDataGenerator.AVRO_SCHEMA;
      StructType structType = AvroConversionUtils.convertAvroSchemaToStructType(schema);
      List<HoodieRecord<?>> records = BenchmarkUtils.generateInputRecords(dataGen, totalRecordsToTest);
      inputDF = BenchmarkUtils.generateInputDf(records, spark);
      inputDF.cache();
      inputDF.count();
      totalFields = inputDF.schema().size();
      encoder = getEncoder(inputDF.schema());

      javaRddRecords = inputDF.toJavaRDD();

      javaRDDHoodieRecords = BenchmarkUtils.convertToDatasetHoodieRecord(inputDF, structType);
      javaRDDHoodieRecords.cache();
      javaRDDHoodieRecords.count();

      datasetHoodieRecords = spark.createDataset(javaRDDHoodieRecords.rdd(), (Encoder<HoodieSparkRecord>)Encoders.bean(HoodieSparkRecord.class));
      datasetHoodieRecords.cache();
      datasetHoodieRecords.count();

      datasetHoodieRecordsKryo = spark.createDataset(javaRDDHoodieRecords.rdd(), (Encoder<HoodieSparkRecord>)Encoders.kryo(HoodieSparkRecord.class));
      datasetHoodieRecordsKryo.cache();
      datasetHoodieRecordsKryo.count();

    } catch (IOException e) {
      e.printStackTrace();
      throw new Exception("Exception thrown while generating records to write ", e);
    }
  }

  private static ExpressionEncoder getEncoder(StructType schema) {
    return SparkAdapterSupport$.MODULE$.sparkAdapter().getCatalystExpressionUtils().getEncoder(schema);
  }
}
