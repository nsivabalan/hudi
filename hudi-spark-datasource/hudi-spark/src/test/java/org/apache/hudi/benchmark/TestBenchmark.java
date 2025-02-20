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
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

public class TestBenchmark {

  @Test
  public void test1() throws IOException {

    SparkSession spark;
    JavaSparkContext jssc;
    FileSystem fs;
    HoodieTestDataGenerator dataGen;
    int totalFields;
    Schema schema;
    Dataset<Row> inputDF;
    ExpressionEncoder encoder;
    JavaRDD<Row> javaRddRecords;
    JavaRDD<HoodieSparkRecord> javaRDDHoodieRecords;
    Dataset<HoodieSparkRecord> datasetHoodieRecords;

    spark = SparkSession.builder().appName("Hoodie Write Benchmark")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").master("local[2]").getOrCreate();
    jssc = new JavaSparkContext(spark.sparkContext());
    spark.sparkContext().setLogLevel("WARN");
    fs = FileSystem.get(jssc.hadoopConfiguration());
    dataGen = new HoodieTestDataGenerator();
    schema = HoodieTestDataGenerator.AVRO_SCHEMA;
    StructType structType = AvroConversionUtils.convertAvroSchemaToStructType(schema);
    totalFields = schema.getFields().size();

    List<HoodieRecord<?>> records = BenchmarkUtils.generateInputRecords(dataGen, 1000);
    inputDF = BenchmarkUtils.generateInputDf(records, spark);
    inputDF.cache();
    inputDF.count();
    totalFields = inputDF.schema().size();
    ExpressionEncoder encoder1 = getEncoder(inputDF.schema());
    ExpressionEncoder encoder2 = getEncoder(inputDF.schema());

    inputDF
        .map(new SparkDfTransformationBenchmark.DataFrameMapFunc(totalFields), encoder1)
        .repartition(10)
        .sort("_row_key")
        .map(new SparkDfTransformationBenchmark.DataFrameMapFunc2(totalFields), encoder2)
        .count();

    /*Properties properties = new Properties();
    properties.setProperty(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "_row_key");
    properties.setProperty(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "partition_path");
    properties.setProperty(DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME().key(),"org.apache.hudi.keygen.SimpleKeyGenerator");
    javaRDDHoodieRecords = BenchmarkUtils.convertToDatasetHoodieRecord(inputDF, properties, structType);
    javaRDDHoodieRecords.cache();
    javaRDDHoodieRecords.count();
    datasetHoodieRecords = spark.createDataset(javaRDDHoodieRecords.rdd(), (Encoder<HoodieSparkRecord>)Encoders.kryo(HoodieSparkRecord.class));
    datasetHoodieRecords.cache();
    datasetHoodieRecords.count();

    datasetHoodieRecords
        .map(new SparkDfTransformationBenchmark.DatasetHoodieRecMapFunc(), Encoders.kryo(HoodieSparkRecord.class))
        .repartition(10)
        //.sort("key.recordKey") // cannot sort based on columns.
        .map(new SparkDfTransformationBenchmark.DatasetHoodieRecMapFunc(), Encoders.kryo(HoodieSparkRecord.class))
        .count();*/

    System.out.println("adsf");
  }

  private static ExpressionEncoder getEncoder(StructType schema) {
    return SparkAdapterSupport$.MODULE$.sparkAdapter().getCatalystExpressionUtils().getEncoder(schema);
  }
}
