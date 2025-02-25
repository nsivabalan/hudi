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

import org.apache.hudi.SparkAdapterSupport$;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieSparkBeanRecord;
import org.apache.hudi.common.model.HoodieSparkBeanUnsafeRowRecord;
import org.apache.hudi.common.model.HoodieSparkRecord;
import org.apache.hudi.common.model.HoodieSparkRowBeanRecord;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.TableNotFoundException;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.UniformReservoir;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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
  private ExpressionEncoder<Row> encoder;
  private JavaRDD<Row> javaRddRecords;
  private JavaRDD<HoodieSparkRecord> javaRDDHoodieRecords;
  private Dataset<HoodieSparkRecord> datasetHoodieRecords;
  private Dataset<HoodieSparkRecord> datasetHoodieRecordsKryo;
  private JavaRDD<HoodieSparkBeanRecord> javaRDDHoodieBeanRecords;
  private Dataset<HoodieSparkBeanRecord> datasetHoodieBeanRecords;
  private Dataset<HoodieSparkBeanUnsafeRowRecord> datasetHoodieSparkBeanUnsafeRowRecords;
  private Dataset<HoodieSparkRowBeanRecord> datasetHoodieRowBeanRecords;

  private JavaRDD<HoodieAvroRecord<?>> javaRDDHoodieAvroRecords;

  private List<Long> dfRunTimes = new ArrayList<>();
  private List<Long> datasetHoodieRecordsRunTimes = new ArrayList<>();
  private List<Long> javaRddRowRunTimes = new ArrayList<>();
  private List<Long> javaRddHoodieRecordsRunTimes = new ArrayList<>();
  private List<Long> javaRddHoodieAvroRecordsRunTimes = new ArrayList<>();

  private List<Long> datasetHoodieBeanRecordsRunTimes = new ArrayList<>();
  private List<Long> javaRddHoodieBeanRecordsRunTimes = new ArrayList<>();
  private List<Long> datasetHoodieRowBeanRecordsRunTimes = new ArrayList<>();

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
    public Integer totalRecordsToTest = 100;

    @Parameter(names = {"--repartition-by-num"}, description = "repatition by num spark tasks", required = false)
    public Integer repartitionByNum = 2;

    @Parameter(names = {"--parallelism-to-generate-input"}, description = "repatition by num spark tasks", required = false)
    public Integer parallelismToGenerateInput = 2;

    @Parameter(names = {"--total-rounds"}, description = "total rounds", required = false)
    public Integer roundsToTest = 2;

    @Parameter(names = {"--enable-sorting"}, description = "Validate column stats for all columns in the schema", required = false)
    public boolean enableSorting = false;

    @Parameter(names = {"--disable-df-benchmark"}, description = "Validate column stats for all columns in the schema", required = false)
    public boolean disableDataFrameBenchmark = false;

    @Parameter(names = {"--disable-dataset-hr-benchmark"}, description = "Validate column stats for all columns in the schema", required = false)
    public boolean disableDatasetHoodieRecordsBenchmark = false;

    @Parameter(names = {"--disable-javardd-row-benchmark"}, description = "Validate column stats for all columns in the schema", required = false)
    public boolean disableJavaRddRowBenchmark = false;

    @Parameter(names = {"--disable-javardd-hr-benchmark"}, description = "Validate column stats for all columns in the schema", required = false)
    public boolean disableJavaRddHoodieRecordsBenchmark = false;

    @Parameter(names = {"--disable-javardd-h-avro-records-benchmark"}, description = "Validate column stats for all columns in the schema", required = false)
    public boolean disableJavaRddHoodieAvroRecordsBenchmark = false;

    @Parameter(names = {"--disable-dataset-hr-bean-benchmark"}, description = "Validate column stats for all columns in the schema", required = false)
    public boolean disableDatasetHoodieRecordsBeanBenchmark = false;

    @Parameter(names = {"--disable-javardd-hr-bean-benchmark"}, description = "Validate column stats for all columns in the schema", required = false)
    public boolean disableJavaRddHoodieRecordsBeanBenchmark = false;

    @Parameter(names = {"--disable-dataset-hoodie-row-bean-benchmark"}, description = "Validate column stats for all columns in the schema", required = false)
    public boolean disableDatasetHoodieRowBeanRecordsBenchmark = false;

    @Parameter(names = {"--disable-dataset-scala-hr-bean-benchmark"}, description = "Validate column stats for all columns in the schema", required = false)
    public boolean disableDatasetScalaHoodieRecordsBeanBenchmark = false;

    @Parameter(names = {"--enable-validation"}, description = "Validate column stats for all columns in the schema", required = false)
    public boolean enableValidation = false;

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
      /*if (!cfg.disableJavaRddRowBenchmark) {
        LOG.warn("JavaRDD row Benchmarking Starting");
        setupJavaRddRows();
        LOG.warn("JavaRDD row Set up complete");
        runJavaRddRowBechmark();
        LOG.warn("JavaRDD row benchmarking complete");
      }*/
      if (!cfg.disableJavaRddHoodieRecordsBenchmark) {
        LOG.warn("JavaRDD HoodieRecords Benchmarking Starting");
        setupJavaRddHoodieRecords();
        LOG.warn("JavaRDD HoodieRecords Set up complete");
        runJavaRDDHoodieRecordsBechmark();
        LOG.warn("JavaRDD HoodieRecords benchmarking complete");
      }

      if (!cfg.disableJavaRddHoodieAvroRecordsBenchmark) {
        LOG.warn("JavaRDD Hoodie Avro Records Benchmarking Starting");
        setupJavaRddHoodieAvroRecords();
        LOG.warn("JavaRDD Hoodie Avro Records Set up complete");
        runJavaRDDHoodieAvroRecordsBechmark();
        LOG.warn("JavaRDD Hoodie Avro Records benchmarking complete");
      }

      /*if (!cfg.disableDatasetHoodieRecordsBenchmark) {
        LOG.warn("Dataset HoodieRecords Benchmarking Starting");
        setupDatasetHoodieRecords();
        LOG.warn("Dataset HoodieRecords Set up complete");
        runDatasetHoodieRecordsBechmark();
        LOG.warn("Dataset HoodieRecords benchmarking complete");
      }*/

      /*if (!cfg.disableJavaRddHoodieRecordsBeanBenchmark) {
        LOG.warn("JavaRDD HoodieRecords Bean Benchmarking Starting");
        setupJavaRddHoodieBeanRecords();
        LOG.warn("JavaRDD HoodieRecords Bean Set up complete");
        runJavaRDDHoodieBeanRecordsBechmark();
        LOG.warn("JavaRDD HoodieRecords Bean benchmarking complete");
      }*/

      /*if (!cfg.disableDatasetHoodieRecordsBeanBenchmark) {
        LOG.warn("Dataset HoodieRecords Bean Benchmarking Starting");
        setupDatasetHoodieBeanRecords();
        LOG.warn("Dataset HoodieRecords Bean Set up complete");
        runDatasetHoodieRecordsBeanBechmark();
        LOG.warn("Dataset HoodieRecords Bean benchmarking complete");
      }*/

      /*if (!cfg.disableDatasetScalaHoodieRecordsBeanBenchmark) {
        LOG.warn("Dataset HoodieRecords Bean Benchmarking Starting");
        setupDatasetScalaHoodieRecordsBean();
        LOG.warn("Dataset HoodieRecords Bean Set up complete");
        //runDatasetHoodieRecordsBeanBechmark();
        LOG.warn("Dataset HoodieRecords Bean benchmarking complete");
      }*/

      /*if (!cfg.disableDatasetHoodieRowBeanRecordsBenchmark) {
        LOG.warn("Dataset HoodieRow Bean Records Benchmarking Starting");
        setupDatasetHoodieRowBeanRecords();
        LOG.warn("Dataset HoodieRow Bean Records Set up complete");
        runDatasetHoodieRecordsBeanBechmark();
        LOG.warn("Dataset HoodieRow Bean Records benchmarking complete");
      }*/

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

      if (!javaRddHoodieAvroRecordsRunTimes.isEmpty()) {
        com.codahale.metrics.Histogram histogram = new com.codahale.metrics.Histogram(new UniformReservoir(1_000_000));
        javaRddHoodieAvroRecordsRunTimes.forEach(entry -> histogram.update(entry));
        LOG.warn("Java RDD HoodieAvroRecord Stats ");
        logStats(histogram);
      }

      if (!javaRddHoodieBeanRecordsRunTimes.isEmpty()) {
        com.codahale.metrics.Histogram histogram = new com.codahale.metrics.Histogram(new UniformReservoir(1_000_000));
        javaRddHoodieBeanRecordsRunTimes.forEach(entry -> histogram.update(entry));
        LOG.warn("Java RDD HoodieRecord Bean Stats ");
        logStats(histogram);
      }

      if (!datasetHoodieBeanRecordsRunTimes.isEmpty()) {
        com.codahale.metrics.Histogram histogram = new com.codahale.metrics.Histogram(new UniformReservoir(1_000_000));
        datasetHoodieBeanRecordsRunTimes.forEach(entry -> histogram.update(entry));
        LOG.warn("Dataset HoodieBeanRecord Stats ");
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
    if (javaRDDHoodieRecords == null) {
      setupJavaRddHoodieRecords();
    }
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

  private void setupJavaRddHoodieAvroRecords() {
    javaRDDHoodieAvroRecords = BenchmarkUtils.convertToDatasetHoodieAvroRecord(inputDF);
    javaRDDHoodieAvroRecords.cache();
    javaRDDHoodieAvroRecords.count();
  }

  private void setupJavaRddHoodieBeanRecords() {
    javaRDDHoodieBeanRecords = BenchmarkUtils.convertToDatasetHoodieBeanRecords(inputDF, structType.fields().length);
    //javaRDDHoodieBeanRecords.cache();
    //javaRDDHoodieBeanRecords.count();
  }

  private void setupDatasetHoodieBeanRecords() {
    if (javaRDDHoodieBeanRecords == null) {
      setupJavaRddHoodieBeanRecords();
    }

    //datasetHoodieBeanRecords = inputDF.map(new MapFuncRowToBean(encoder), Encoders.kryo(HoodieSparkBeanRecord.class));

    //RDD<HoodieSparkRecordBean> localRdd = BenchmarkUtils.convertToDatasetHoodieRecordCustomEncoderRdd(inputDF, structType);
    datasetHoodieBeanRecords = engineContext.getSqlContext().sparkSession().createDataset(javaRDDHoodieBeanRecords.rdd(), Encoders.bean(HoodieSparkBeanRecord.class));
    datasetHoodieBeanRecords.cache(); // unsafeRow is empty array here.
    datasetHoodieBeanRecords.count();
    /*datasetHoodieSparkBeanUnsafeRowRecords = datasetHoodieBeanRecords.map(new DatasetHoodieSparkBeanToSparkBeanUnsafeRowRecord(), Encoders.kryo(HoodieSparkBeanUnsafeRowRecord.class));
    datasetHoodieSparkBeanUnsafeRowRecords.cache();
    datasetHoodieSparkBeanUnsafeRowRecords.count();*/
  }

  /*private void setupDatasetScalaHoodieRecordsBean() {
    datasetScalaHoodieBeanRecords = inputDF.map(new MapFuncRowToScalaSparkBeanRec(encoder), BenchmarkUtils.getEncoderForScalaSparkRecord());

    //RDD<HoodieSparkRecordBean> localRdd = BenchmarkUtils.convertToDatasetHoodieRecordCustomEncoderRdd(inputDF, structType);
    //datasetHoodieRecordsBean = engineContext.getSqlContext().sparkSession().createDataset(javaRDDHoodieRecordsBean.rdd(), Encoders.bean(HoodieSparkBeanRecord.class));
    datasetScalaHoodieBeanRecords.cache(); // unsafeRow is empty array here.
    datasetScalaHoodieBeanRecords.count();
  }*/

  private void setupDatasetHoodieRowBeanRecords() {
    datasetHoodieRowBeanRecords = inputDF.map(new MapFuncRowToSparkRowBeanRecord(), Encoders.bean(HoodieSparkRowBeanRecord.class));

    datasetHoodieRowBeanRecords.cache(); // unsafeRow is empty array here.
    datasetHoodieRowBeanRecords.count();
  }

  private ExpressionEncoder<Row> getEncoder(StructType schema) {
    return SparkAdapterSupport$.MODULE$.sparkAdapter().getCatalystExpressionUtils().getEncoder(schema);
  }


  /**
   * @throws Exception
   */

  public void runDfBechmark() throws Exception {
    for (int i = 0; i < cfg.roundsToTest; i++) {
      LOG.warn("  Running iteration " + i);
      long startTime = System.currentTimeMillis();
      if (cfg.enableSorting) {
        inputDF
            .repartition(cfg.repartitionByNum)
            .sort("key") // disabling to get perf nos across the board
            .repartition(cfg.repartitionByNum * 3)
            .map(new DataFrameMapFunc(totalFields), encoder)
            .count();
      } else {
        inputDF
            .repartition(cfg.repartitionByNum)
            .map(new DataFrameMapFunc(totalFields), encoder)
            .count();
      }
      long totalTime = System.currentTimeMillis() - startTime;
      LOG.warn("   Iteration " + i + " took " + totalTime);
      dfRunTimes.add(totalTime);
    }
  }

  public void runDfBechmark2() throws Exception {
    for (int i = 0; i < cfg.roundsToTest; i++) {
      LOG.warn("  Running iteration " + i);
      long startTime = System.currentTimeMillis();
      if (cfg.enableSorting) {
        inputDF
            .repartition(cfg.repartitionByNum)
            .sort("key") // disabling to get perf nos across the board
            .repartition(cfg.repartitionByNum * 3)
            .map(new DataFrameMapFunc(totalFields), encoder)
            //.map(new SparkDfTransformationBenchmark.DataFrameMapFunc(totalFields), bs.encoder) // enabling another map call fails.
            .count();
      } else {
        inputDF
            .repartition(cfg.repartitionByNum)
            .map(new DataFrameMapFunc(totalFields), encoder)
            .count();
      }
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

  public void runDatasetHoodieRecordsBeanBechmark() throws Exception {
    for (int i = 0; i < cfg.roundsToTest; i++) {
      LOG.warn("  Running iteration " + i);
      long startTime = System.currentTimeMillis();
      if (cfg.enableSorting) {
        datasetHoodieBeanRecords
            .repartition(cfg.repartitionByNum)
            .sort("key.recordKey")
            .repartition(cfg.repartitionByNum * 3)
            .map(new DatasetHoodieRecBeanMapFunc(), Encoders.bean(HoodieSparkBeanRecord.class))
            .count();
      } else {
        datasetHoodieBeanRecords
            .repartition(cfg.repartitionByNum)
            .map(new DatasetHoodieRecBeanMapFunc(), Encoders.bean(HoodieSparkBeanRecord.class))
            .count();
      }

      if (cfg.enableValidation) {
        /*List<HoodieSparkBeanRecord> firstList = datasetHoodieBeanRecords
            .repartition(cfg.repartitionByNum)
            .map(new DatasetHoodieRecBeanMapFunc(), Encoders.bean(HoodieSparkBeanRecord.class))
            //.sort("key.recordKey") // sorting by cols not supported. there is no sort by support.
            //.map(new DatasetHoodieRecMapFunc(), Encoders.kryo(HoodieSparkRecord.class)) // disabling to get perf nos across the board
            .collectAsList();
        List<Row> inputList = inputDF.collectAsList();

        List<HoodieSparkBeanUnsafeRowRecord> secondList = datasetHoodieBeanRecords
            .repartition(cfg.repartitionByNum)
            .map(new DatasetHoodieRecBeanMapFunc(), Encoders.bean(HoodieSparkBeanRecord.class))
            .map(new DatasetHoodieSparkBeanToSparkBeanUnsafeRowRecord(), Encoders.kryo(HoodieSparkBeanUnsafeRowRecord.class)).collectAsList();
        Map<String, String> inputMap = new HashMap<>();
        inputList.forEach(entry -> inputMap.put(entry.getString(0), entry.getString(3)));

        Map<String, String> outputMap = new HashMap<>();
        secondList.forEach(entry -> outputMap.put(entry.getKey().getRecordKey(), entry.getData().getString(3)));

        AtomicInteger missingKeys = new AtomicInteger();
        inputMap.forEach((k, v) -> {
          if (outputMap.containsKey(k)) {
            ValidationUtils.checkArgument(v.equals(outputMap.get(k)));
          } else {
            missingKeys.getAndIncrement();
          }
        });*/
      }

      long totalTime = System.currentTimeMillis() - startTime;
      LOG.warn("   Iteration " + i + " took " + totalTime);
      datasetHoodieBeanRecordsRunTimes.add(totalTime);
    }

      /*LOG.warn("------------------- Starting to validate records --------------------");

      List<HoodieSparkRecordBean> inputRecords = datasetHoodieRecordsBean.collectAsList();

      List<HoodieSparkRecordBean> outputRecords = datasetHoodieRecordsBean
          .repartition(cfg.repartitionByNum)
          .map(new DatasetHoodieRecBeanMapFunc(), Encoders.bean(HoodieSparkRecordBean.class)).collectAsList();
      Map<HoodieKey, HoodieSparkRecordBean> inputMap = new HashMap<>();
      inputRecords.forEach(entry -> inputMap.put(entry.getKey(), entry));

      Map<HoodieKey, HoodieSparkRecordBean> outputMap = new HashMap<>();
      outputRecords.forEach(entry -> outputMap.put(entry.getKey(), entry));
      int missingRecords = 0;
      for(Map.Entry<HoodieKey, HoodieSparkRecordBean> entry: inputMap.entrySet()) {
        if (outputMap.containsKey(entry.getKey())) {
          ValidationUtils.checkArgument(entry.getValue().equals(outputMap.get(entry.getKey())));
        } else {
          LOG.warn("Missing " + entry.getKey());
          missingRecords++;
        }
      }*/
    if (cfg.enableValidation) {

        /*Option<HoodieUnsafeRowUtils.NestedFieldPath> fieldRef = HoodieUnsafeRowUtils.composeNestedFieldPath(structType, "textField");

        // validate that InternalRow and UnsafeRow matches.
        Map<HoodieKey, InternalRow> inputMap1 = new HashMap<>();
        javaRDDHoodieRecords.collect().forEach(entry -> inputMap1.put(entry.getKey(), entry.getData()));

        Map<HoodieKey, InternalRow> outputMap1 = new HashMap<>();
        datasetHoodieBeanRecords
            .repartition(cfg.repartitionByNum)
            .map(new DatasetHoodieRecBeanMapFunc(), Encoders.bean(HoodieSparkBeanRecord.class))
            .collectAsList()
            .forEach(entry -> outputMap1.put(entry.getKey(), entry.getData()));

        inputMap1.entrySet().forEach(entry -> {
          ValidationUtils.checkArgument(outputMap1.containsKey(entry.getKey()));
          System.out.println("Sample value " + HoodieUnsafeRowUtils.getNestedInternalRowValue(entry.getValue(), fieldRef.get()));
          ValidationUtils.checkArgument(HoodieUnsafeRowUtils.getNestedInternalRowValue(entry.getValue(), fieldRef.get())
                  .equals(HoodieUnsafeRowUtils.getNestedInternalRowValue(outputMap1.get(entry.getKey()), fieldRef.get())));
        });*/
    }
  }

  public void runDatasetHoodieRecordsBeanBechmark2() throws Exception {
    for (int i = 0; i < cfg.roundsToTest; i++) {
      LOG.warn("  Running iteration " + i);
      long startTime = System.currentTimeMillis();
      if (cfg.enableSorting) {
        datasetHoodieBeanRecords
            //.map(new DatasetHoodieRecBeanMapFunc(), Encoders.bean(HoodieSparkBeanRecord.class))
            .repartition(cfg.repartitionByNum)
            .sort("key.recordKey") // sorting by cols not supported. there is no sort by support.
            //.map(new DatasetHoodieRecMapFunc(), Encoders.kryo(HoodieSparkRecord.class)) // disabling to get perf nos across the board
            .repartition(cfg.repartitionByNum * 3)
            .map(new DatasetHoodieSparkBeanToSparkBeanUnsafeRowRecord(), Encoders.kryo(HoodieSparkBeanUnsafeRowRecord.class))
            .count();
      } else {
        datasetHoodieBeanRecords
            //.map(new DatasetHoodieRecBeanMapFunc(), Encoders.bean(HoodieSparkBeanRecord.class))
            .repartition(cfg.repartitionByNum)
            .map(new DatasetHoodieSparkBeanToSparkBeanUnsafeRowRecord(), Encoders.kryo(HoodieSparkBeanUnsafeRowRecord.class))
            .count();
      }

      long totalTime = System.currentTimeMillis() - startTime;
      LOG.warn("   Iteration " + i + " took " + totalTime);
      datasetHoodieBeanRecordsRunTimes.add(totalTime);
    }
  }

  public void runDatasetHoodierowBeanRecordsBechmark() throws Exception {
    for (int i = 0; i < cfg.roundsToTest; i++) {
      LOG.warn("  Running iteration " + i);
      long startTime = System.currentTimeMillis();
      datasetHoodieRowBeanRecords
          .repartition(cfg.repartitionByNum)
          .map(new DatasetHoodieRowBeanRecMapFunc(), Encoders.bean(HoodieSparkRowBeanRecord.class))
          //.sort("key.recordKey") // sorting by cols not supported. there is no sort by support.
          //.map(new DatasetHoodieRecMapFunc(), Encoders.kryo(HoodieSparkRecord.class)) // disabling to get perf nos across the board
          .count();
      long totalTime = System.currentTimeMillis() - startTime;
      LOG.warn("   Iteration " + i + " took " + totalTime);
      datasetHoodieRowBeanRecordsRunTimes.add(totalTime);
    }

      /*LOG.warn("------------------- Starting to validate records --------------------");

      List<HoodieSparkRecordBean> inputRecords = datasetHoodieRecordsBean.collectAsList();

      List<HoodieSparkRecordBean> outputRecords = datasetHoodieRecordsBean
          .repartition(cfg.repartitionByNum)
          .map(new DatasetHoodieRecBeanMapFunc(), Encoders.bean(HoodieSparkRecordBean.class)).collectAsList();
      Map<HoodieKey, HoodieSparkRecordBean> inputMap = new HashMap<>();
      inputRecords.forEach(entry -> inputMap.put(entry.getKey(), entry));

      Map<HoodieKey, HoodieSparkRecordBean> outputMap = new HashMap<>();
      outputRecords.forEach(entry -> outputMap.put(entry.getKey(), entry));
      int missingRecords = 0;
      for(Map.Entry<HoodieKey, HoodieSparkRecordBean> entry: inputMap.entrySet()) {
        if (outputMap.containsKey(entry.getKey())) {
          ValidationUtils.checkArgument(entry.getValue().equals(outputMap.get(entry.getKey())));
        } else {
          LOG.warn("Missing " + entry.getKey());
          missingRecords++;
        }
      }*/
    if (cfg.enableValidation) {

      // validate that InternalRow and UnsafeRow matches.
      Map<String, Row> inputMap1 = new HashMap<>();
      inputDF.collectAsList().forEach(entry -> inputMap1.put(entry.getString(0), entry));

      Map<String, Row> outputMap1 = new HashMap<>();
      datasetHoodieRowBeanRecords
          .repartition(cfg.repartitionByNum)
          .map(new DatasetHoodieRowBeanRecMapFunc(), Encoders.bean(HoodieSparkRowBeanRecord.class))
          .collectAsList()
          .forEach(entry -> outputMap1.put(entry.getKey().getRecordKey(), entry.getData()));

      inputMap1.entrySet().forEach(entry -> {
        ValidationUtils.checkArgument(outputMap1.containsKey(entry.getKey()));
        ValidationUtils.checkArgument(entry.getValue().getString(3).equals(outputMap1.get(entry.getKey()).getString(3)));
      });
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
      if (cfg.enableSorting) {
        javaRDDHoodieRecords
            .repartition(cfg.repartitionByNum)
            .sortBy(new JavaRDDHoodieRecordsSortFunc(), true, cfg.repartitionByNum) // disabling to get perf nos across the board
            //.map(new JavaRDDHoodieRecordsMapFunc()) // disabling to get perf nos across the board
            .repartition(cfg.repartitionByNum * 3)
            .map(new JavaRDDHoodieRecordsMapFunc())
            .count();
      } else {
        javaRDDHoodieRecords
            .repartition(cfg.repartitionByNum)
            .map(new JavaRDDHoodieRecordsMapFunc())
            .count();
      }
      long totalTime = System.currentTimeMillis() - startTime;
      LOG.warn("   Iteration " + i + " took " + totalTime);
      javaRddHoodieRecordsRunTimes.add(totalTime);
    }
  }

  public void runJavaRDDHoodieAvroRecordsBechmark() throws Exception {

    for (int i = 0; i < cfg.roundsToTest; i++) {
      LOG.warn("  Running iteration " + i);
      long startTime = System.currentTimeMillis();
      if (cfg.enableSorting) {
        javaRDDHoodieAvroRecords
            .repartition(cfg.repartitionByNum)
            .sortBy(new JavaRDDHoodieAvroRecordsSortFunc(), true, cfg.repartitionByNum) // disabling to get perf nos across the board
            //.map(new JavaRDDHoodieRecordsMapFunc()) // disabling to get perf nos across the board
            .repartition(cfg.repartitionByNum * 3)
            .map(new JavaRDDHoodieAvroRecordsMapFunc())
            .count();
      } else {
        javaRDDHoodieAvroRecords
            .repartition(cfg.repartitionByNum)
            .map(new JavaRDDHoodieAvroRecordsMapFunc())
            .count();
      }
      long totalTime = System.currentTimeMillis() - startTime;
      LOG.warn("   Iteration " + i + " took " + totalTime);
      javaRddHoodieAvroRecordsRunTimes.add(totalTime);
    }
  }

  public void runJavaRDDHoodieBeanRecordsBechmark() throws Exception {

    for (int i = 0; i < cfg.roundsToTest; i++) {
      LOG.warn("  Running iteration " + i);
      long startTime = System.currentTimeMillis();
      javaRDDHoodieBeanRecords
          .repartition(cfg.repartitionByNum)
          .map(new JavaRDDHoodieRecordsBeanMapFunc())
          //.sortBy(new JavaRDDHoodieRecordsSortFunc(), true, numPartitions) // disabling to get perf nos across the board
          //.map(new JavaRDDHoodieRecordsMapFunc()) // disabling to get perf nos across the board
          .count();
      long totalTime = System.currentTimeMillis() - startTime;
      LOG.warn("   Iteration " + i + " took " + totalTime);
      javaRddHoodieBeanRecordsRunTimes.add(totalTime);
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

  static class MapFuncRowToBean implements MapFunction<Row, HoodieSparkBeanRecord> {

    private final ExpressionEncoder<Row> encoder;

    public MapFuncRowToBean(ExpressionEncoder<Row> encoder) {
      this.encoder = encoder;
    }

    @Override
    public HoodieSparkBeanRecord call(Row row) throws Exception {
      HoodieSparkBeanRecord hoodieSparkBeanRecord = new HoodieSparkBeanRecord();
      hoodieSparkBeanRecord.setKey(new HoodieKey(row.getString(0), row.getString(1)));
      //hoodieSparkBeanRecord.setData((UnsafeRow) encoder.createSerializer().apply(row));
      return hoodieSparkBeanRecord;
    }
  }

  /*static class MapFuncRowToScalaSparkBeanRec implements MapFunction<Row, ScalaSparkRecord> {

    private final ExpressionEncoder<Row> encoder;
    public MapFuncRowToScalaSparkBeanRec(ExpressionEncoder<Row> encoder) {
      this.encoder = encoder;
    }

    @Override
    public ScalaSparkRecord call(Row row) throws Exception {
      ScalaSparkRecord hoodieSparkBeanRecord = BenchmarkUtils.getScalaSparkRecord(new ScalaHoodieKey(row.getString(0), row.getString(1)),
          encoder.createSerializer().apply(row));
      return hoodieSparkBeanRecord;
    }
  }*/

  static class MapFuncRowToSparkRowBeanRecord implements MapFunction<Row, HoodieSparkRowBeanRecord> {

    public MapFuncRowToSparkRowBeanRecord() {

    }

    @Override
    public HoodieSparkRowBeanRecord call(Row row) throws Exception {
      HoodieSparkRowBeanRecord hoodieSparkRowBeanRecord = new HoodieSparkRowBeanRecord();
      hoodieSparkRowBeanRecord.setKey(new HoodieKey(row.getString(0), row.getString(1)));
      hoodieSparkRowBeanRecord.setData(row);
      hoodieSparkRowBeanRecord.setCopy(false);
      return hoodieSparkRowBeanRecord;
    }
  }

  static class DatasetHoodieRecBeanMapFunc implements MapFunction<HoodieSparkBeanRecord, HoodieSparkBeanRecord> {

    @Override
    public HoodieSparkBeanRecord call(HoodieSparkBeanRecord hoodieSparkRecord) throws Exception {
      //InternalRow genericInternalRow = hoodieSparkRecord.getData();
      return hoodieSparkRecord;
    }
  }

  static class DatasetHoodieRowBeanRecMapFunc implements MapFunction<HoodieSparkRowBeanRecord, HoodieSparkRowBeanRecord> {

    @Override
    public HoodieSparkRowBeanRecord call(HoodieSparkRowBeanRecord hoodieSparkRecord) throws Exception {
      Row row = hoodieSparkRecord.getData();
      return hoodieSparkRecord;
    }
  }

  static class DatasetHoodieSparkBeanToSparkBeanUnsafeRowRecord implements MapFunction<HoodieSparkBeanRecord, HoodieSparkBeanUnsafeRowRecord> {

    public DatasetHoodieSparkBeanToSparkBeanUnsafeRowRecord() {
    }

    @Override
    public HoodieSparkBeanUnsafeRowRecord call(HoodieSparkBeanRecord hoodieSparkRecord) throws Exception {
      HoodieSparkBeanUnsafeRowRecord toReturn = new HoodieSparkBeanUnsafeRowRecord();
      toReturn.setKey(hoodieSparkRecord.getKey());
      byte[] bytes = hoodieSparkRecord.getData();
      Kryo kryo = new Kryo();
      Input input = new Input(bytes);
      toReturn.setData((UnsafeRow) kryo.readClassAndObject(input));
      return toReturn;
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
      InternalRow row = (UnsafeRow) hoodieRecord.getData();
      return row.getString(1);
    }
  }

  static class JavaRDDHoodieAvroRecordsSortFunc implements Function<HoodieAvroRecord<?>, String> {
    @Override
    public String call(HoodieAvroRecord hoodieRecord) throws Exception {
      OverwriteWithLatestAvroPayload row = (OverwriteWithLatestAvroPayload) hoodieRecord.getData();
      return hoodieRecord.getRecordKey();
    }
  }

  static class JavaRDDHoodieRecordsMapFunc implements Function<HoodieSparkRecord, HoodieSparkRecord> {

    @Override
    public HoodieSparkRecord call(HoodieSparkRecord hoodieRecord) throws Exception {
      InternalRow row = (UnsafeRow) hoodieRecord.getData();
      return hoodieRecord;
    }
  }

  static class JavaRDDHoodieAvroRecordsMapFunc implements Function<HoodieAvroRecord<?>, HoodieAvroRecord<?>> {

    @Override
    public HoodieAvroRecord call(HoodieAvroRecord hoodieRecord) throws Exception {
      OverwriteWithLatestAvroPayload record = (OverwriteWithLatestAvroPayload) hoodieRecord.getData();
      return hoodieRecord;
    }
  }

  static class JavaRDDHoodieRecordsBeanMapFunc implements Function<HoodieSparkBeanRecord, HoodieSparkBeanRecord> {

    @Override
    public HoodieSparkBeanRecord call(HoodieSparkBeanRecord hoodieRecord) throws Exception {
      //InternalRow row = hoodieRecord.getData();
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


