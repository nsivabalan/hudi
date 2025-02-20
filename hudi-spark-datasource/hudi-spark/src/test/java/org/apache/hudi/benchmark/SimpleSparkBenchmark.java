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

package org.apache.hudi.benchmark;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class SimpleSparkBenchmark {

  private SparkSession spark;
  private Dataset<Row> dataset;

  public static class Config implements Serializable {
    @Parameter(names = {"--run-id"}, description = "id for this run of the benchmark")
    public String runId = "run-" + System.currentTimeMillis();

    @Parameter(names = {"--input-path"}, description = "id for this run of the benchmark")
    public String inputPath = "s3a://ethan-lakehouse-us-west-2/siva_testing/row-writer1/chennai/dbec6477-056a-4fbb-a7a6-6f6f656370ba-0_2-93-314_20250220194741951.parquet";

    @Parameter(names = {"--help", "-h"}, help = true)
    public Boolean help = false;

    public static ChainedOptionsBuilder prepareOpts(ChainedOptionsBuilder opts, Config cfg) {
      return opts
          .param("runId", cfg.runId)
          .result(cfg.runId + ".csv")
          .resultFormat(ResultFormatType.CSV);
    }
  }

  @Setup(Level.Trial)
  public void setup() {
    spark = SparkSession.builder()
        .appName("Spark JMH Benchmark")
        .master("yarn")  // Use "yarn" to run on YARN cluster
        .config("spark.submit.deployMode", "local") // Or "client"
        .config("spark.executor.memory", "2g")
        .config("spark.executor.instances", "2")
        .getOrCreate();

    dataset = spark.read().parquet("s3a://ethan-lakehouse-us-west-2/siva_testing/row-writer1/chennai/dbec6477-056a-4fbb-a7a6-6f6f656370ba-0_2-93-314_20250220194741951.parquet");
  }

  @Benchmark
  public void benchmarkAggregation() {
    dataset.repartition(6).count();
    dataset.repartition(6).groupBy("rider").count().collect();
  }

  /*@Benchmark
  public void benchmarkFilter() {
    dataset.filter("column2 > 100").count();
  }*/

  @TearDown(Level.Trial)
  public void tearDown() {
    if (spark != null) {
      spark.stop();
    }
  }

  public static void main(String[] args) throws RunnerException {
    SimpleSparkBenchmark.Config cfg = new SimpleSparkBenchmark.Config();
    JCommander cmd = new JCommander(cfg, null, args);
    if (cfg.help) {
      cmd.usage();
      System.exit(1);
    }

    System.err.println("Starting " + SimpleSparkBenchmark.class.getSimpleName());
    ChainedOptionsBuilder opt = Config.prepareOpts(
        new OptionsBuilder().include(SimpleSparkBenchmark.class.getSimpleName())
        , cfg);
    new Runner(opt.build()).run();
  }
}
