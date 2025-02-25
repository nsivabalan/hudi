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

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.UniformReservoir;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class TestRowWritingBenchmarkTool {

  private static final Logger LOG = LoggerFactory.getLogger(TestRowWritingBenchmarkTool.class);

  @Test
  public void simpleTest() {
    RowWritingBenchmarkTool.Config config = new RowWritingBenchmarkTool.Config();
    SparkConf sparkConf = UtilHelpers.buildSparkConf("Table-Size-Stats", "local[2]");
    sparkConf.set("spark.executor.memory", "1g");
    JavaSparkContext jsc = new JavaSparkContext(sparkConf);

    new RowWritingBenchmarkTool(jsc, config).run();
    /*List<Integer> latencies = Arrays.asList(new Integer[]{143439, 149489, 158096, 166255, 167712, 168988, 163400, 156235, 169264, 159793, 179844, 155156, 162144, 173845, 171401});

    //145679, 151857, 159033, 172698, 159313, 168772, 967752, 203016, 165118, 201824, 172519, 155940, 159679, 171156, 168830, 167567, 169233, 179235, 166592,

    com.codahale.metrics.Histogram histogram = new com.codahale.metrics.Histogram(new UniformReservoir(1_000_000));
    latencies.forEach(entry -> histogram.update(entry));
    logStats(histogram);*/

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
}
