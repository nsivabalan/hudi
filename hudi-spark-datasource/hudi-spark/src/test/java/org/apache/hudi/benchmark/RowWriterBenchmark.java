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
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.BenchmarkUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.types.StructType;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 1, time = 60)
@Measurement(iterations = 10)
@Fork(value = 1, warmups = 1, jvmArgs = {"-Xms4G", "-Xmx4G"})
public class RowWriterBenchmark {

  private static final Logger LOG = LoggerFactory.getLogger(RowWriterBenchmark.class);

  public static class Config implements Serializable {
    @Parameter(names = {"--run-id"}, description = "id for this run of the benchmark")
    public String runId = "run-" + System.currentTimeMillis();

    @Parameter(names = {"--help", "-h"}, help = true)
    public Boolean help = false;

    @Parameter(names = {"--total-records-to-test"}, description = "total records to test", required = false)
    public Integer totalRecordsToTest = 10000;

    @Parameter(names = {"--repartition-by-num"}, description = "repatition by num spark tasks", required = false)
    public Integer repartitionByNum = 10;

    public static ChainedOptionsBuilder prepareOpts(ChainedOptionsBuilder opts, Config cfg) {
      return opts
          .param("runId", cfg.runId)
          .result(cfg.runId + ".csv")
          .resultFormat(ResultFormatType.CSV);
    }
  }

  @State(Scope.Benchmark)
  public static class RowWriterBenchState {

    @Param({""})
    public String runId;

    @Param({"1000000"})
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

    @Setup(Level.Trial)
    public void setup() {
      try {
        jssc = buildJSSC("Hoodie Write Benchmark", "yarn");
        spark = SparkSession.builder().config(jssc.getConf()).getOrCreate();
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
        javaRddRecords.cache();
        javaRddRecords.count();

        javaRDDHoodieRecords = BenchmarkUtils.convertToDatasetHoodieRecord(inputDF, structType);
        javaRDDHoodieRecords.cache();
        javaRDDHoodieRecords.count();

        datasetHoodieRecords = spark.createDataset(javaRDDHoodieRecords.rdd(), (Encoder<HoodieSparkRecord>) Encoders.bean(HoodieSparkRecord.class));
        //datasetHoodieRecords.cache();
        //datasetHoodieRecords.count();

        datasetHoodieRecordsKryo = spark.createDataset(javaRDDHoodieRecords.rdd(), (Encoder<HoodieSparkRecord>) Encoders.kryo(HoodieSparkRecord.class));
        datasetHoodieRecordsKryo.cache();
        datasetHoodieRecordsKryo.count();

      } catch (IOException e) {
        e.printStackTrace();
        throw new HoodieException("Exception thrown while generating records to write ", e);
      }
    }

    private ExpressionEncoder getEncoder(StructType schema) {
      return SparkAdapterSupport$.MODULE$.sparkAdapter().getCatalystExpressionUtils().getEncoder(schema);
    }

    @TearDown(Level.Trial)
    public void teardown() {
      try {
        spark.stop();
      } catch (Exception e) {
        throw new RuntimeException("Error tearing down benchmark", e);
      }
    }

    private static JavaSparkContext buildJSSC(String appName, String defaultMaster) {
      final SparkConf sparkConf = new SparkConf().setAppName(appName);
      sparkConf.setMaster("yarn");
      sparkConf.set("spark.submit.deployMode","cluster");
      /*sparkConf.setIfMissing("spark.ui.port", "8090");
      sparkConf.setIfMissing("spark.driver.maxResultSize", "2g");
      sparkConf.setIfMissing("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
      sparkConf.setIfMissing("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar");
      sparkConf.setIfMissing("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension");
      sparkConf.setIfMissing("spark.hadoop.mapred.output.compress", "true");
      sparkConf.setIfMissing("spark.hadoop.mapred.output.compression.codec", "true");
      sparkConf.setIfMissing("spark.hadoop.mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
      sparkConf.setIfMissing("spark.hadoop.mapred.output.compression.type", "BLOCK");*/

      LOG.warn("AAA Spark executor memory " + sparkConf.get("spark.executor.memory","100g"));
      LOG.warn("AAA Spark executor num instances " + sparkConf.get("spark.executor.instances","10"));

      sparkConf.set("spark.driver.memory","6g");
      sparkConf.set("spark.executor.memory","10g");
      sparkConf.set("spark.executor.instances","3");
      sparkConf.set("spark.executor.cores","3");
      sparkConf.set("spark.driver.cores","3");

      LOG.warn("BBB Spark executor memory " + sparkConf.get("spark.executor.memory","100g"));
      LOG.warn("BBB Spark executor num instances " + sparkConf.get("spark.executor.instances","10"));

      return new JavaSparkContext(sparkConf);
    }
  }

  @Benchmark
  public void runDfBechmark(RowWriterBenchState bs, Blackhole bh) throws Exception {
    int totalFields = bs.totalFields;
    bs.inputDF
        .repartition(bs.repartitionByNum)
        .map(new DataFrameMapFunc(totalFields), bs.encoder)
        .sort("_row_key") // disabling to get perf nos across the board
        //.map(new SparkDfTransformationBenchmark.DataFrameMapFunc(totalFields), bs.encoder) // enabling another map call fails.
        .count();
  }

  /*@Benchmark
  public void runDatasetHoodieRecordsBechmark(RowWriterBenchState bs, Blackhole bh) throws Exception {
    int totalFields = bs.totalFields;
    bs.datasetHoodieRecordsKryo
        .repartition(bs.repartitionByNum)
        .map(new DatasetHoodieRecMapFunc(), Encoders.kryo(HoodieSparkRecord.class))
        //.sort("key.recordKey") // sorting by cols not supported. there is no sort by support.
        //.map(new DatasetHoodieRecMapFunc(), Encoders.kryo(HoodieSparkRecord.class)) // disabling to get perf nos across the board
        .count();
  }

  @Benchmark
  public void runJavaRddRowBechmark(RowWriterBenchState bs, Blackhole bh) throws Exception {
    int totalFields = bs.totalFields;
    int numPartitions = bs.repartitionByNum;

    bs.javaRddRecords
        .repartition(numPartitions)
        .map(new JavaRddRowMapFunc(bs.totalFields))
        //.sortBy(new JavaRddRowSortFunc(), true, numPartitions) // disabling to get perf nos across the board
        //.map(new JavaRddRowMapFunc(plan.totalFields)) // disabling to get perf nos across the board
        .count();
  }

  @Benchmark
  public void runJavaRDDHoodieRecordsBechmark(RowWriterBenchState bs, Blackhole bh) throws Exception {
    int totalFields = bs.totalFields;
    int numPartitions = bs.repartitionByNum;

    bs.javaRDDHoodieRecords
        .repartition(numPartitions)
        .map(new JavaRDDHoodieRecordsMapFunc())
        //.sortBy(new JavaRDDHoodieRecordsSortFunc(), true, numPartitions) // disabling to get perf nos across the board
        //.map(new JavaRDDHoodieRecordsMapFunc()) // disabling to get perf nos across the board
        .count();
  }*/

  public static void main(String[] args) throws RunnerException {
    RowWriterBenchmark.Config cfg = new RowWriterBenchmark.Config();
    JCommander cmd = new JCommander(cfg, null, args);
    if (cfg.help) {
      cmd.usage();
      System.exit(1);
    }

    System.err.println("Starting " + RowWriterBenchmark.class.getSimpleName());
    ChainedOptionsBuilder opt = RowWriterBenchmark.Config.prepareOpts(
        new OptionsBuilder().include(RowWriterBenchmark.class.getSimpleName())
        , cfg);
    new Runner(opt.build()).run();
  }

  static class DatasetHoodieRecMapFunc implements MapFunction<HoodieSparkRecord, HoodieSparkRecord> {

    @Override
    public HoodieSparkRecord call(HoodieSparkRecord hoodieSparkRecord) throws Exception {
      InternalRow internalRow = hoodieSparkRecord.getData();
      internalRow.getString(1);
      return hoodieSparkRecord;
    }
  }

  static class DatasetHoodieRecPairMapFunc implements MapFunction<HoodieSparkRecord, Pair<String, HoodieSparkRecord>> {

    @Override
    public Pair<String, HoodieSparkRecord> call(HoodieSparkRecord hoodieSparkRecord) throws Exception {
      InternalRow internalRow = hoodieSparkRecord.getData();
      internalRow.getString(1);
      return Pair.of(hoodieSparkRecord.getRecordKey(), hoodieSparkRecord);
    }
  }

  static class JavaRddRowMapFunc implements Function<Row, Row> {

    int totalFields;

    JavaRddRowMapFunc(int totalFields) {
      this.totalFields = totalFields;
    }

    @Override
    public Row call(Row row) throws Exception {
      return recreateRow(row, totalFields);
    }
  }

  static class JavaRddRowSortFunc implements Function<Row, String> {

    @Override
    public String call(Row row) throws Exception {
      return (String) row.get(1);
    }
  }

  static class JavaRDDHoodieRecordsSortFunc implements Function<HoodieSparkRecord, String> {
    @Override
    public String call(HoodieSparkRecord hoodieRecord) throws Exception {
      UnsafeRow row = (UnsafeRow) hoodieRecord.getData();
      return row.getString(1);
    }
  }

  static class JavaRDDHoodieRecordsMapFunc implements Function<HoodieSparkRecord, HoodieSparkRecord> {

    @Override
    public HoodieSparkRecord call(HoodieSparkRecord hoodieRecord) throws Exception {
      UnsafeRow row = (UnsafeRow) hoodieRecord.getData();
      return hoodieRecord;
    }
  }

  static class DataFrameMapFunc implements MapFunction<Row, Row> {

    int totalFields;

    DataFrameMapFunc(int totalFields) {
      this.totalFields = totalFields;
    }

    @Override
    public Row call(Row row) throws Exception {
      return recreateRow(row, totalFields);
      //return row;
    }
  }

  /*static class DataFrameMapFunc2 implements MapFunction<Row, Row> {
    int totalFields;
    DataFrameMapFunc2(int totalFields) {
      this.totalFields = totalFields;
    }

    @Override
    public Row call(Row row) throws Exception {
      return row;
    }
  }*/

  private static Row recreateRow(Row row, int totalFields) {
    Object[] vals = new Object[totalFields];
    int counter = 0;
    while (counter < totalFields) {
      vals[counter] = row.get(counter);
      counter++;
    }
    return RowFactory.create(vals);
  }
}
