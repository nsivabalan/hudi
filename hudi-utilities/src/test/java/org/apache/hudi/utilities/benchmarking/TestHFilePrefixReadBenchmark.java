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

package org.apache.hudi.utilities.benchmarking;

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;

/**
 * Test cases for {@link MetadataBenchmarkingTool}.
 */
public class TestHFilePrefixReadBenchmark {

  private static final Logger LOG = LoggerFactory.getLogger(TestHFilePrefixReadBenchmark.class);
  private static SparkSession sparkSession;

  @BeforeAll
  public static void setUpClass() {
    // Initialize SparkSession for tests
    sparkSession = SparkSession.builder()
        .appName("TestMetadataBenchmarkingTool")
        .master("local[2]")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
        .getOrCreate();

    LOG.info("SparkSession and EngineContext initialized for tests");
  }

  @AfterAll
  public static void tearDownClass() {
    if (sparkSession != null) {
      sparkSession.stop();
      sparkSession = null;
      LOG.info("SparkSession stopped");
    }
  }

  @Test
  public void testHFilePrefixReadBenchmark(@TempDir Path tempDir) throws Exception {
    LOG.info("Running MetadataBenchmarkingTool test with temp directory: {}", tempDir);

    // Create config for MetadataBenchmarkingTool with 2 columns (tenantID & age)
    HFilePrefixReadBenchmark.Config config = new HFilePrefixReadBenchmark.Config();
    config.outputDir = "/tmp/hfile";
    HFilePrefixReadBenchmark prefixReadBenchmark = new HFilePrefixReadBenchmark(sparkSession, config);
    prefixReadBenchmark.run();
  }
}

