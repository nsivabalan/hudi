/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.utilities;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;
import org.apache.hudi.utilities.functional.TestHoodieSnapshotExporter;

import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;

public class TestPrintRecordsTool extends SparkClientFunctionalTestHarness {

  static final Logger LOG = LoggerFactory.getLogger(TestHoodieSnapshotExporter.class);
  static final int NUM_RECORDS = 100;
  static final String COMMIT_TIME = "20200101000000";
  static final String COMMIT_TIME2 = "20200101000010";
  static final String PARTITION_PATH = "2020";
  static final String TABLE_NAME = "testing";
  String sourcePath;
  String targetPath;
  LocalFileSystem lfs;

  @BeforeEach
  public void init() throws Exception {
    // Initialize test data dirs
    sourcePath = Paths.get(basePath(), "source").toString();
    targetPath = Paths.get(basePath(), "target").toString();
    lfs = (LocalFileSystem) HadoopFSUtils.getFs(basePath(), jsc().hadoopConfiguration());

    HoodieTableMetaClient.withPropertyBuilder()
        .setTableType(HoodieTableType.MERGE_ON_READ)
        .setTableName(TABLE_NAME)
        .setPayloadClass(HoodieAvroPayload.class)
        .initTable(jsc().hadoopConfiguration(), sourcePath);

    // Prepare data as source Hudi dataset
    HoodieWriteConfig cfg = getHoodieWriteConfig(sourcePath);
    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(cfg)) {
      writeClient.startCommitWithTime(COMMIT_TIME);
      HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(new String[] {PARTITION_PATH});
      List<HoodieRecord> records = dataGen.generateInserts(COMMIT_TIME, NUM_RECORDS);
      JavaRDD<HoodieRecord> recordsRDD = jsc().parallelize(records, 1);
      writeClient.bulkInsert(recordsRDD, COMMIT_TIME);

      writeClient.startCommitWithTime(COMMIT_TIME2);
      writeClient.upsert(recordsRDD, COMMIT_TIME2);
    }
  }

  @Test
  public void simpleTest() {

    PrintRecordsTool.Config cfg = new PrintRecordsTool.Config();
    cfg.basePath = sourcePath;
    cfg.partitionPath = PARTITION_PATH;
    String dataFileToTest = null;
    String logFileToTest = null;
    RemoteIterator<LocatedFileStatus> itr = null;
    try {
      itr = lfs.listFiles(new Path(sourcePath + "/2020/"), true);
      while (itr.hasNext()) {
        Path dataFile = itr.next().getPath();
        LOG.info(">>> Prepared test file: " + dataFile);
        if (dataFile.getName().endsWith(".parquet")) {
          dataFileToTest = dataFile.getName();
        }
        if (dataFile.getName().contains(".log")) {
          logFileToTest = dataFile.getName();
        }
        if (dataFileToTest != null && logFileToTest != null) {
          break;
        }
      }

      cfg.fileId = FSUtils.getFileId(dataFileToTest);
      cfg.baseInstantTime = FSUtils.getCommitTime(dataFileToTest);

      PrintRecordsTool printRecordsTool = new PrintRecordsTool(jsc(), cfg);
      HoodieSparkEngineContext context = new HoodieSparkEngineContext(jsc());
      HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setConf(jsc().hadoopConfiguration()).setBasePath(cfg.basePath)
          .setLoadActiveTimelineOnLoad(true).build();
      HoodieWriteConfig writeConfig = getHoodieWriteConfig(sourcePath);
      printRecordsTool.printRecs(writeConfig, context, metaClient);

      cfg.logFiles = logFileToTest;
      cfg.newBaseParquetFile = dataFileToTest;
      cfg.compareRecords = true;

      printRecordsTool.printRecs(writeConfig, context, metaClient);

    } catch (FileNotFoundException ex) {
      ex.printStackTrace();
    } catch (IOException ex) {
      ex.printStackTrace();
    }
  }

  private HoodieWriteConfig getHoodieWriteConfig(String basePath) {
    return HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withEmbeddedTimelineServerEnabled(false)
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
        .withParallelism(2, 2)
        .withBulkInsertParallelism(2)
        .withDeleteParallelism(2)
        .forTable(TABLE_NAME)
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(0).build())
        .build();
  }
}
