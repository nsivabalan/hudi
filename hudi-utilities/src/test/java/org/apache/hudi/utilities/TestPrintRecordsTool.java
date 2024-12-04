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
  public void simpleTest() throws Exception {

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
      printRecordsTool.printRecs(writeConfig, context, metaClient, jsc().hadoopConfiguration());

      cfg.logFiles = logFileToTest;
      cfg.newBaseParquetFile = dataFileToTest;
      cfg.compareRecords = true;

      printRecordsTool.printRecs(writeConfig, context, metaClient, jsc().hadoopConfiguration());

    } catch (FileNotFoundException ex) {
      ex.printStackTrace();
    } catch (IOException ex) {
      ex.printStackTrace();
    }
  }

  /**
   * delete matched. 2, 4, 5, 7, 9, 11, 13 , 15
   * rollback blocks: 3, 6, 8, 10, 12, 14
   * <p>
   * 1: 134 records. delete block: 2281 records. none of them matches the record key of interest.
   * 2: 20240915200809984. 71
   * 2: 20240915200809984. 1589 records in delete block.
   * 3: RB. target instant time ROLLBACK_BLOCK. matches 2
   * 4: 20240915203755867. 71
   * 4: 20240915203755867. 1589 records in delete block.
   * 4 succeeded.
   * 5: 20240916020817335, 59
   * 5: 20240916020817335, 285 records in delete block.
   * 6: rollback  target instant time matches 5
   * 7: 20240916022517864, 59
   * 7: 20240916022517864, 285 records in delete block.
   * 8: rollback  target instant time matches 7
   * 9: 20240916030258627, 59
   * 9: 20240916030258627, 285 records in delete block.
   * 10: rollback. target instant time matches
   * 11: 20240916032112303, 59
   * 11: 20240916032112303, 285 records in delete block.
   * 12: rollback. target instant time matches 11
   * 13: 20240916035133285, 59
   * 13: 20240916035133285, 285 records in delete block.
   * 14: rollback. target instant time matches 14
   * 15: 20240916055242213, 59
   * 15: 20240916055242213. 290 records to delete.
   */
  @Test
  public void circle_user_debug() {

    PrintRecordsTool.Config cfg = new PrintRecordsTool.Config();
    cfg.basePath = "/tmp/circle_user/tbl_path/";
    cfg.partitionPath = "date_updated_at=2024-09-10";
    cfg.recordKey = "611bfb28dd08cf1a21c9bc4f";
    cfg.fileId = "13903032-ee6e-4f68-b8ba-e4257c715c76-0";
    cfg.baseInstantTime = "20240915075008505";
    cfg.colsToPrint = "_hoodie_partition_path,_hoodie_commit_time,_hoodie_file_name,_hoodie_commit_seqno";
    cfg.propsFilePath = "/tmp/input.props";
    cfg.printLogBlocksInfo = true;

    PrintRecordsTool printRecordsTool = new PrintRecordsTool(jsc(), cfg);
    printRecordsTool.run();
  }

  @Test
  public void circle_user_debug1() {

    PrintRecordsTool.Config cfg = new PrintRecordsTool.Config();
    cfg.basePath = "/tmp/tiramisu_investigate/tbl_path";
    cfg.partitionPath = "date_updated_at=2024-09-12";
    cfg.recordKey = "63dbdd60260d549722ecb758";
    cfg.fileId = "f3a5c4ef-4ca6-41de-9502-72e1e78f84f0-0";
    cfg.baseInstantTime = "20240914175524391";
    cfg.colsToPrint = "_hoodie_partition_path,_hoodie_commit_time,_hoodie_file_name,_hoodie_commit_seqno";
    cfg.propsFilePath = "/tmp/input.props";
    cfg.printLogBlocksInfo = true;

    PrintRecordsTool printRecordsTool = new PrintRecordsTool(jsc(), cfg);
    printRecordsTool.run();
  }

  @Test
  public void circle_user_debug_2024_04_24() {

    PrintRecordsTool.Config cfg = new PrintRecordsTool.Config();
    cfg.basePath = "/tmp/tiramisu_investigate/tbl_path";
    cfg.partitionPath = "date_updated_at=2024-04-24";
    cfg.recordKey = "63dbdd60260d549722ecb758";
    cfg.fileId = "eec0b1ad-11dd-43f4-bdf6-8f696500d544-0";
    cfg.baseInstantTime = "20240424094534253";
    cfg.colsToPrint = "_hoodie_partition_path,_hoodie_commit_time,_hoodie_file_name,_hoodie_commit_seqno";
    cfg.propsFilePath = "/tmp/input.props";

    PrintRecordsTool printRecordsTool = new PrintRecordsTool(jsc(), cfg);
    printRecordsTool.run();
  }

  @Test
  public void circle_user_debug_2024_09_14() {

    PrintRecordsTool.Config cfg = new PrintRecordsTool.Config();
    cfg.basePath = "/tmp/tiramisu_investigate/tbl_path";
    cfg.partitionPath = "date_updated_at=2024-09-14";
    cfg.recordKey = "63dbdd60260d549722ecb758";
    cfg.fileId = "f73407df-486d-4e24-94e5-60b410f509d5-0";
    cfg.baseInstantTime = "20240914234855060";
    cfg.colsToPrint = "_hoodie_partition_path,_hoodie_commit_time,_hoodie_file_name,_hoodie_commit_seqno";
    cfg.propsFilePath = "/tmp/input.props";

    PrintRecordsTool printRecordsTool = new PrintRecordsTool(jsc(), cfg);
    printRecordsTool.run();
  }

  @Test
  public void circle_user_debug2() {

    PrintRecordsTool.Config cfg = new PrintRecordsTool.Config();
    cfg.basePath = "/tmp/circle_user/tbl_path/";
    cfg.partitionPath = "date_updated_at=2024-09-10";
    cfg.recordKey = "611bfb28dd08cf1a21c9bc4f";
    cfg.fileId = "13903032-ee6e-4f68-b8ba-e4257c715c76-0";
    cfg.baseInstantTime = "20240915075008505";
    cfg.colsToPrint = "_hoodie_partition_path,_hoodie_commit_time,_hoodie_file_name,_hoodie_commit_seqno";
    cfg.propsFilePath = "/tmp/input.props";
    // cfg.printLogBlocksInfo = true;
    //cfg.logFiles =
      //  "/tmp/circle_user/tbl_path/date_updated_at=2024-09-10/.13903032-ee6e-4f68-b8ba-e4257c715c76-0_20240915075008505.log.1_1450-2038-309104,/tmp/circle_user/tbl_path/date_updated_at=2024-09-10/.13903032-ee6e-4f68-b8ba-e4257c715c76-0_20240915075008505.log.2_1397-2213-301658,/tmp/circle_user/tbl_path/date_updated_at=2024-09-10/.13903032-ee6e-4f68-b8ba-e4257c715c76-0_20240915075008505.log.3_1-0-1,/tmp/circle_user/tbl_path/date_updated_at=2024-09-10/.13903032-ee6e-4f68-b8ba-e4257c715c76-0_20240915075008505.log.4_1397-7487-1076753,/tmp/circle_user/tbl_path/date_updated_at=2024-09-10/.13903032-ee6e-4f68-b8ba-e4257c715c76-0_20240915075008505.log.5_811-50063-6404861,/tmp/circle_user/tbl_path/date_updated_at=2024-09-10/.13903032-ee6e-4f68-b8ba-e4257c715c76-0_20240915075008505.log.6_1-0-1,/tmp/circle_user/tbl_path/date_updated_at=2024-09-10/.13903032-ee6e-4f68-b8ba-e4257c715c76-0_20240915075008505.log.7_811-53727-6909994,/tmp/circle_user/tbl_path/date_updated_at=2024-09-10/.13903032-ee6e-4f68-b8ba-e4257c715c76-0_20240915075008505.log.8_1-0-1,/tmp/circle_user/tbl_path/date_updated_at=2024-09-10/.13903032-ee6e-4f68-b8ba-e4257c715c76-0_20240915075008505.log.9_811-539-87049,/tmp/circle_user/tbl_path/date_updated_at=2024-09-10/.13903032-ee6e-4f68-b8ba-e4257c715c76-0_20240915075008505.log.10_1-0-1,/tmp/circle_user/tbl_path/date_updated_at=2024-09-10/.13903032-ee6e-4f68-b8ba-e4257c715c76-0_20240915075008505.log.11_811-4174-539857,/tmp/circle_user/tbl_path/date_updated_at=2024-09-10/.13903032-ee6e-4f68-b8ba-e4257c715c76-0_20240915075008505.log.12_1-0-1,/tmp/circle_user/tbl_path/date_updated_at=2024-09-10/.13903032-ee6e-4f68-b8ba-e4257c715c76-0_20240915075008505.log.13_811-12064-1486394,/tmp/circle_user/tbl_path/date_updated_at=2024-09-10/.13903032-ee6e-4f68-b8ba-e4257c715c76-0_20240915075008505.log.14_1-0-1,/tmp/circle_user/tbl_path/date_updated_at=2024-09-10/.13903032-ee6e-4f68-b8ba-e4257c715c76-0_20240915075008505.log.15_822-41116-4957529";
    cfg.logFiles =
        "/tmp/circle_user/tbl_path/date_updated_at=2024-09-10/.13903032-ee6e-4f68-b8ba-e4257c715c76-0_20240915075008505.log.1_1450-2038-309104,/tmp/circle_user/tbl_path/date_updated_at=2024-09-10/.13903032-ee6e-4f68-b8ba-e4257c715c76-0_20240915075008505.log.2_1397-2213-301658,/tmp/circle_user/tbl_path/date_updated_at=2024-09-10/.13903032-ee6e-4f68-b8ba-e4257c715c76-0_20240915075008505.log.3_1-0-1,/tmp/circle_user/tbl_path/date_updated_at=2024-09-10/.13903032-ee6e-4f68-b8ba-e4257c715c76-0_20240915075008505.log.4_1397-7487-1076753";

    PrintRecordsTool printRecordsTool = new PrintRecordsTool(jsc(), cfg);
    printRecordsTool.run();
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
