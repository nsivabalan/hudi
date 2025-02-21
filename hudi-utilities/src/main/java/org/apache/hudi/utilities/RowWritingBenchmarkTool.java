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

package org.apache.hudi.utilities;

import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.SparkAdapterSupport$;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.function.SerializableFunction;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieSparkRecord;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.TableNotFoundException;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.UniformReservoir;
import org.apache.avro.Schema;
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
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class RowWritingBenchmarkTool implements Serializable {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(RowWritingBenchmarkTool.class);

  // Spark context
  private JavaSparkContext jsc;
  // config
  private Config cfg;
  // Properties with source, hoodie client, key generator etc.
  private TypedProperties props;
  private HoodieSparkEngineContext engineContext;

  private HoodieTestDataGenerator dataGen;
  private int totalFields;
  private Schema schema;
  private StructType structType;
  private Dataset<Row> inputDF;
  private ExpressionEncoder encoder;
  private JavaRDD<Row> javaRddRecords;
  private JavaRDD<HoodieSparkRecord> javaRDDHoodieRecords;
  private Dataset<HoodieSparkRecord> datasetHoodieRecords;
  private Dataset<HoodieSparkRecord> datasetHoodieRecordsKryo;
  private List<Long> dfRunTimes = new ArrayList<>();
  private List<Long> datasetHoodieRecordsRunTimes = new ArrayList<>();
  private List<Long> javaRddRowRunTimes = new ArrayList<>();
  private List<Long> javaRddHoodieRecordsRunTimes = new ArrayList<>();

  public RowWritingBenchmarkTool(JavaSparkContext jsc, Config cfg) {
    this.jsc = jsc;
    this.cfg = cfg;
    engineContext = new HoodieSparkEngineContext(jsc);
    this.props = UtilHelpers.buildProperties(cfg.configs);
  }

  public static class Config implements Serializable {

    @Parameter(names = {"--base-path-to-store-input"}, description = "Base path to store input", required = false)
    public String basePathToStoreInput = "";

    @Parameter(names = {"--total-records-to-test"}, description = "total records to test", required = false)
    public Integer totalRecordsToTest = 10000;

    @Parameter(names = {"--repartition-by-num"}, description = "repatition by num spark tasks", required = false)
    public Integer repartitionByNum = 10;

    @Parameter(names = {"--parallelism-to-generate-input"}, description = "repatition by num spark tasks", required = false)
    public Integer parallelismToGenerateInput = 10;

    @Parameter(names = {"--total-rounds"}, description = "total rounds", required = false)
    public Integer roundsToTest = 2;

    @Parameter(names = {"--disable-df-benchmark"}, description = "Validate column stats for all columns in the schema", required = false)
    public boolean disableDataFrameBenchmark = false;

    @Parameter(names = {"--disable-dataset-hr-benchmark"}, description = "Validate column stats for all columns in the schema", required = false)
    public boolean disableDatasetHoodieRecordsBenchmark = false;

    @Parameter(names = {"--disable-javardd-row-benchmark"}, description = "Validate column stats for all columns in the schema", required = false)
    public boolean disableJavaRddRowBenchmark = false;

    @Parameter(names = {"--disable-javardd-hr-benchmark"}, description = "Validate column stats for all columns in the schema", required = false)
    public boolean disableJavaRddHoodieRecordsBenchmark = false;

    @Parameter(names = {"--spark-master", "-ms"}, description = "Spark master", required = false)
    public String sparkMaster = null;

    @Parameter(names = {"--spark-memory", "-sm"}, description = "spark memory to use", required = false)
    public String sparkMemory = "1g";

    @Parameter(names = {"--hoodie-conf"}, description = "Any configuration that can be set in the properties file "
        + "(using the CLI parameter \"--props\") can also be passed command line using this parameter. This can be repeated",
        splitter = IdentitySplitter.class)
    public List<String> configs = new ArrayList<>();

    @Parameter(names = {"--help", "-h"}, help = true)
    public Boolean help = false;

    @Override
    public String toString() {
      return "RowWritingBenchmarkTool {\n"
          + "   --total_records " + totalRecordsToTest + ", \n"
          + "   --spark-partitions " + repartitionByNum + ", \n"
          + "   --rounds-to-test " + roundsToTest + ", \n"
          + "   --hoodie-conf " + configs
          + "\n}";
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Config config = (Config) o;
      return totalRecordsToTest.equals(config.totalRecordsToTest)
          && Objects.equals(repartitionByNum, config.repartitionByNum)
          && Objects.equals(roundsToTest, config.roundsToTest)
          && Objects.equals(sparkMaster, config.sparkMaster)
          && Objects.equals(sparkMemory, config.sparkMemory)
          && Objects.equals(configs, config.configs);
    }

    @Override
    public int hashCode() {
      return Objects.hash(totalRecordsToTest, repartitionByNum, roundsToTest, sparkMaster, sparkMemory, configs, help);
    }
  }

  public static void main(String[] args) {
    final Config cfg = new Config();
    JCommander cmd = new JCommander(cfg, null, args);

    if (cfg.help || args.length == 0) {
      cmd.usage();
      System.exit(1);
    }

    SparkConf sparkConf = UtilHelpers.buildSparkConf("Table-Size-Stats", cfg.sparkMaster);
    sparkConf.set("spark.executor.memory", cfg.sparkMemory);
    JavaSparkContext jsc = new JavaSparkContext(sparkConf);

    try {
      RowWritingBenchmarkTool rowWritingBenchmarkTool = new RowWritingBenchmarkTool(jsc, cfg);
      rowWritingBenchmarkTool.run();
    } catch (TableNotFoundException e) {
      LOG.warn(String.format("The Hudi data table is not found: [%s].", cfg.totalRecordsToTest), e);
    } catch (Throwable throwable) {
      LOG.error("Failed to get table size stats for " + cfg, throwable);
    } finally {
      jsc.stop();
    }
  }

  public void run() {
    try {
      LOG.info(cfg.toString());
      LOG.warn(" ****** Running benchmark ******");
      if (!cfg.disableDataFrameBenchmark) {
        LOG.warn("DF Benchmarking Starting");
        setupDf();
        LOG.warn("DF Set up complete");
        runDfBechmark();
        LOG.warn("DF benchmarking complete");
      }
      if (!cfg.disableJavaRddRowBenchmark) {
        LOG.warn("JavaRDD row Benchmarking Starting");
        setupJavaRddRows();
        LOG.warn("JavaRDD row Set up complete");
        runJavaRddRowBechmark();
        LOG.warn("JavaRDD row benchmarking complete");
      }
      if (!cfg.disableJavaRddHoodieRecordsBenchmark) {
        LOG.warn("JavaRDD HoodieRecords Benchmarking Starting");
        setupJavaRddHoodieRecords();
        LOG.warn("JavaRDD HoodieRecords Set up complete");
        runJavaRDDHoodieRecordsBechmark();
        LOG.warn("JavaRDD HoodieRecords benchmarking complete");
      }
      if (!cfg.disableDatasetHoodieRecordsBenchmark) {
        LOG.warn("Dataset HoodieRecords Benchmarking Starting");
        setupDatasetHoodieRecords();
        LOG.warn("Dataset HoodieRecords Set up complete");
        runDatasetHoodieRecordsBechmark();
        LOG.warn("Dataset HoodieRecords benchmarking complete");
      }

      if (!dfRunTimes.isEmpty()) {
        com.codahale.metrics.Histogram histogram = new com.codahale.metrics.Histogram(new UniformReservoir(1_000_000));
        dfRunTimes.forEach(entry -> histogram.update(entry));
        LOG.warn("Df benchmarking Stats ");
        logStats(histogram);
      }
      if (!datasetHoodieRecordsRunTimes.isEmpty()) {
        com.codahale.metrics.Histogram histogram = new com.codahale.metrics.Histogram(new UniformReservoir(1_000_000));
        datasetHoodieRecordsRunTimes.forEach(entry -> histogram.update(entry));
        LOG.warn("Dataset HoodieRecords benchmarking Stats ");
        logStats(histogram);
      }

      if (!javaRddRowRunTimes.isEmpty()) {
        com.codahale.metrics.Histogram histogram = new com.codahale.metrics.Histogram(new UniformReservoir(1_000_000));
        javaRddRowRunTimes.forEach(entry -> histogram.update(entry));
        LOG.warn("Java RDD Row Stats ");
        logStats(histogram);
      }

      if (!javaRddHoodieRecordsRunTimes.isEmpty()) {
        com.codahale.metrics.Histogram histogram = new com.codahale.metrics.Histogram(new UniformReservoir(1_000_000));
        javaRddHoodieRecordsRunTimes.forEach(entry -> histogram.update(entry));
        LOG.warn("Java RDD HoodieRecord Stats ");
        logStats(histogram);
      }

    } catch (Exception e) {
      throw new HoodieException("Failed to execution job ", e);
    } finally {
      engineContext.getJavaSparkContext().stop();
    }
  }

  private static void logStats(Histogram histogram) {
    Snapshot snapshot = histogram.getSnapshot();
    LOG.warn("Number of runs: {}", snapshot.size());
    //LOG.info("Total size: {}", Arrays.stream(snapshot.getValues()).sum());
    LOG.warn("Minimum latency: {}", snapshot.getMin());
    LOG.warn("Maximum latency: {}", snapshot.getMax());
    LOG.warn("Average latency: {}", snapshot.getMean());
    LOG.warn("Median latency: {}", snapshot.getMedian());
    LOG.warn("P50 latency: {}", snapshot.getValue(0.5));
    LOG.warn("P90 latency: {}", snapshot.getValue(0.9));
    LOG.warn("P95 latency: {}", snapshot.getValue(0.95));
    LOG.warn("P99 latency: {}", snapshot.getValue(0.99));
  }

  private void setupDf() {
    inputDF = BenchmarkUtils.generateInput(engineContext.getSqlContext().sparkSession(), cfg.parallelismToGenerateInput, cfg.totalRecordsToTest);
    /*dataGen = new HoodieTestDataGenerator();
    schema = HoodieTestDataGenerator.AVRO_SCHEMA;
    if (cfg.totalRecordsToTest <= 100000) {
      List<HoodieRecord<?>> records = BenchmarkUtils.generateInputRecords(dataGen, cfg.totalRecordsToTest);
      inputDF = BenchmarkUtils.generateInputDf(records, engineContext.getSqlContext().sparkSession());
    } else {
      int totalBatches = cfg.totalRecordsToTest / 100000;
      for (int j = 0; j < totalBatches; j++) {
        String pathPerRound = cfg.basePathToStoreInput + "/" + j;
        BenchmarkUtils.generateInputDf(
            BenchmarkUtils.generateInputRecords(dataGen, 100000), engineContext.getSqlContext().sparkSession()).write().format("parquet").save(pathPerRound);
      }
     // inputDF = engineContext.getSqlContext().sparkSession().read().format("parquet").load(cfg.basePathToStoreInput);
    //}*/
    inputDF.cache();
    inputDF.count();
    this.structType = inputDF.schema();
    totalFields = inputDF.schema().size();
    encoder = getEncoder(inputDF.schema());
  }

  /*private Dataset<Row> generateInput() {
    inputDf = BenchmarkUtils.generateInput(engineContext.getSqlContext().sparkSession(), cfg.repartitionByNum, cfg.totalRecordsToTest);
  }*/

  private void setupDatasetHoodieRecords() {
    datasetHoodieRecordsKryo = engineContext.getSqlContext().sparkSession().createDataset(javaRDDHoodieRecords.rdd(), (Encoder<HoodieSparkRecord>) Encoders.kryo(HoodieSparkRecord.class));
    datasetHoodieRecordsKryo.cache();
    datasetHoodieRecordsKryo.count();
  }

  private void setupJavaRddRows() {
    javaRddRecords = inputDF.toJavaRDD();
    javaRddRecords.cache();
    javaRddRecords.count();
  }

  private void setupJavaRddHoodieRecords() {
    javaRDDHoodieRecords = BenchmarkUtils.convertToDatasetHoodieRecord(inputDF, structType);
    javaRDDHoodieRecords.cache();
    javaRDDHoodieRecords.count();
  }

  private ExpressionEncoder getEncoder(StructType schema) {
    return SparkAdapterSupport$.MODULE$.sparkAdapter().getCatalystExpressionUtils().getEncoder(schema);
  }

  public void runDfBechmark() throws Exception {
    for (int i = 0; i < cfg.roundsToTest; i++) {
      LOG.warn("  Running iteration " + i);
      long startTime = System.currentTimeMillis();
      engineContext.getSqlContext().sparkSession().time(() -> inputDF
          .repartition(cfg.repartitionByNum)
          .map(new DataFrameMapFunc(totalFields), encoder)
          //.sort("_row_key") // disabling to get perf nos across the board
          //.map(new SparkDfTransformationBenchmark.DataFrameMapFunc(totalFields), bs.encoder) // enabling another map call fails.
          .count());
      long totalTime = System.currentTimeMillis() - startTime;
      LOG.warn("   Iteration " + i + " took " + totalTime);
      dfRunTimes.add(totalTime);
    }
  }

  public void runDatasetHoodieRecordsBechmark() throws Exception {
    for (int i = 0; i < cfg.roundsToTest; i++) {
      LOG.warn("  Running iteration " + i);
      long startTime = System.currentTimeMillis();
      datasetHoodieRecordsKryo
          .repartition(cfg.repartitionByNum)
          .map(new DatasetHoodieRecMapFunc(), Encoders.kryo(HoodieSparkRecord.class))
          //.sort("key.recordKey") // sorting by cols not supported. there is no sort by support.
          //.map(new DatasetHoodieRecMapFunc(), Encoders.kryo(HoodieSparkRecord.class)) // disabling to get perf nos across the board
          .count();
      long totalTime = System.currentTimeMillis() - startTime;
      LOG.warn("   Iteration " + i + " took " + totalTime);
      datasetHoodieRecordsRunTimes.add(totalTime);
    }
  }

  public void runJavaRddRowBechmark() throws Exception {
    for (int i = 0; i < cfg.roundsToTest; i++) {
      LOG.warn("  Running iteration " + i);
      long startTime = System.currentTimeMillis();
      javaRddRecords
          .repartition(cfg.repartitionByNum)
          .map(new JavaRddRowMapFunc(totalFields))
          //.sortBy(new JavaRddRowSortFunc(), true, numPartitions) // disabling to get perf nos across the board
          //.map(new JavaRddRowMapFunc(plan.totalFields)) // disabling to get perf nos across the board
          .count();
      long totalTime = System.currentTimeMillis() - startTime;
      LOG.warn("   Iteration " + i + " took " + totalTime);
      javaRddRowRunTimes.add(totalTime);
    }
  }

  public void runJavaRDDHoodieRecordsBechmark() throws Exception {
    for (int i = 0; i < cfg.roundsToTest; i++) {
      LOG.warn("  Running iteration " + i);
      long startTime = System.currentTimeMillis();
      javaRDDHoodieRecords
          .repartition(cfg.repartitionByNum)
          .map(new JavaRDDHoodieRecordsMapFunc())
          //.sortBy(new JavaRDDHoodieRecordsSortFunc(), true, numPartitions) // disabling to get perf nos across the board
          //.map(new JavaRDDHoodieRecordsMapFunc()) // disabling to get perf nos across the board
          .count();
      long totalTime = System.currentTimeMillis() - startTime;
      LOG.warn("   Iteration " + i + " took " + totalTime);
      javaRddHoodieRecordsRunTimes.add(totalTime);
    }
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
      //return recreateRow(row, totalFields);
      return row;
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
      //return recreateRow(row, totalFields);
      return row;
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


