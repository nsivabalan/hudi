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

import org.apache.hudi.common.model.HoodieSparkRecord;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;

public class SparkDfTransformationBenchmark {

  // works
  @Fork(value = 1)
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @Warmup(iterations = 1)
  @Measurement(iterations = 1)
  @OutputTimeUnit(TimeUnit.SECONDS)
  public void benchmarkDataframeBenchmark(DfBenchmarkExecutionPlan plan) throws Exception {
    int totalFields = plan.totalFields;
    plan.inputDF
        .repartition(plan.repartitionByNum)
        .map(new DataFrameMapFunc(totalFields), plan.encoder)
        .sort("_row_key") // disabling to get perf nos across the board
        .map(new DataFrameMapFunc(totalFields), plan.encoder) // enabling another map call fails.
        .count();
  }

  // does not work
  /*@Fork(value = 1)
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @Warmup(iterations = 1)
  @Measurement(iterations = 1)
  @OutputTimeUnit(TimeUnit.SECONDS)
  public void benchmarkDatasetHoodieRecordsBenchmark(DfBenchmarkExecutionPlan plan) throws Exception {
    int totalFields = plan.totalFields;
    plan.datasetHoodieRecords
        .repartition(plan.repartitionByNum)
        .map(new DatasetHoodieRecMapFunc(), Encoders.bean(HoodieSparkRecord.class))
        //.sort("key.recordKey")
        //.map(new DatasetHoodieRecMapFunc(), Encoders.bean(HoodieSparkRecord.class))
        .count();
  }*/

  // works
  @Fork(value = 1)
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @Warmup(iterations = 1)
  @Measurement(iterations = 1)
  @OutputTimeUnit(TimeUnit.SECONDS)
  public void benchmarkDatasetHoodieRecordsKryoBenchmark(DfBenchmarkExecutionPlan plan) throws Exception {
    int totalFields = plan.totalFields;
    plan.datasetHoodieRecordsKryo
        .repartition(plan.repartitionByNum)
        .map(new DatasetHoodieRecMapFunc(), Encoders.kryo(HoodieSparkRecord.class))
        //.sort("key.recordKey") // sorting by cols not supported. there is no sort by support.
        //.map(new DatasetHoodieRecMapFunc(), Encoders.kryo(HoodieSparkRecord.class)) // disabling to get perf nos across the board
        .count();
  }

  // works
  @Fork(value = 1)
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @Warmup(iterations = 1)
  @Measurement(iterations = 1)
  @OutputTimeUnit(TimeUnit.SECONDS)
  public void benchmarkJavaRddRowBenchmark(DfBenchmarkExecutionPlan plan) throws Exception {
    int totalFields = plan.totalFields;
    int numPartitions = plan.repartitionByNum;

    plan.javaRddRecords
        .repartition(numPartitions)
        .map(new JavaRddRowMapFunc(plan.totalFields))
        //.sortBy(new JavaRddRowSortFunc(), true, numPartitions) // disabling to get perf nos across the board
        //.map(new JavaRddRowMapFunc(plan.totalFields)) // disabling to get perf nos across the board
        .count();
  }

  // works
  @Fork(value = 1)
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @Warmup(iterations = 1)
  @Measurement(iterations = 1)
  @OutputTimeUnit(TimeUnit.SECONDS)
  public void benchmarkJavaRddHoodieRecordBenchmark(DfBenchmarkExecutionPlan plan) throws Exception {
    int totalFields = plan.totalFields;
    int numPartitions = plan.repartitionByNum;

    plan.javaRDDHoodieRecords
        .repartition(numPartitions)
        .map(new JavaRDDHoodieRecordsMapFunc())
        //.sortBy(new JavaRDDHoodieRecordsSortFunc(), true, numPartitions) // disabling to get perf nos across the board
        //.map(new JavaRDDHoodieRecordsMapFunc()) // disabling to get perf nos across the board
        .count();
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
      //return recreateRow(row, totalFields);
      return row;
    }
  }

  static class DataFrameMapFunc2 implements MapFunction<Row, Row> {

    int totalFields;

    DataFrameMapFunc2(int totalFields) {
      this.totalFields = totalFields;
    }

    @Override
    public Row call(Row row) throws Exception {
      return row;
    }
  }

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
