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

package org.apache.hudi.io.storage;

import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.bloom.BloomFilterFactory;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.testutils.HoodieClientTestHarness;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import static org.apache.hudi.testutils.HoodieDatasetTestUtils.ENCODER;
import static org.apache.hudi.testutils.HoodieDatasetTestUtils.STRUCT_TYPE;
import static org.apache.hudi.testutils.HoodieDatasetTestUtils.getConfigBuilder;
import static org.apache.hudi.testutils.HoodieDatasetTestUtils.getRandomRows;
import static org.apache.hudi.testutils.HoodieDatasetTestUtils.toInternalRows;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit tests {@link HoodieInternalRowParquetWriter}.
 */
public class TestHoodieInternalRowParquetWriter extends HoodieClientTestHarness {

  private static final Random RANDOM = new Random();

  @BeforeEach
  public void setUp() throws Exception {
    initSparkContexts("TestHoodieInternalRowParquetWriter");
    initPath();
    initFileSystem();
    initTestDataGenerator();
    initMetaClient();
  }

  @AfterEach
  public void tearDown() throws Exception {
    cleanupResources();
  }

  @Test
  public void endToEndTest() throws IOException {
    HoodieWriteConfig cfg = getConfigBuilder(basePath).build();
    for (int i = 0; i < 5; i++) {
      // init write support and parquet config
      HoodieRowParquetWriteSupport writeSupport = getWriteSupport(cfg, hadoopConf);
      HoodieRowParquetConfig parquetConfig = new HoodieRowParquetConfig(writeSupport,
          CompressionCodecName.SNAPPY, cfg.getParquetBlockSize(), cfg.getParquetPageSize(), cfg.getParquetMaxFileSize(),
          writeSupport.getHadoopConf(), cfg.getParquetCompressionRatio());

      // prepare path
      String fileId = UUID.randomUUID().toString();
      Path filePath = new Path(basePath + "/" + fileId);
      String partitionPath = HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH;
      metaClient.getFs().mkdirs(new Path(basePath));

      // init writer
      HoodieInternalRowParquetWriter writer = new HoodieInternalRowParquetWriter(filePath, parquetConfig);

      // generate input
      int size = 10 + RANDOM.nextInt(100);
      // Generate inputs
      Dataset<Row> inputRows = getRandomRows(sqlContext, size, partitionPath, false);
      List<InternalRow> internalRows = toInternalRows(inputRows, ENCODER);

      // issue writes
      for (InternalRow internalRow : internalRows) {
        writer.write(internalRow);
      }

      // close the writer
      writer.close();

      // verify rows
      Dataset<Row> result = sqlContext.read().parquet(basePath);
      assertEquals(0, inputRows.except(result).count());
    }
  }

  private HoodieRowParquetWriteSupport getWriteSupport(HoodieWriteConfig writeConfig, Configuration hadoopConf) {
    BloomFilter filter = BloomFilterFactory.createBloomFilter(
        writeConfig.getBloomFilterNumEntries(),
        writeConfig.getBloomFilterFPP(),
        writeConfig.getDynamicBloomFilterMaxNumEntries(),
        writeConfig.getBloomFilterType());
    return new HoodieRowParquetWriteSupport(hadoopConf, STRUCT_TYPE, filter);
  }
}
