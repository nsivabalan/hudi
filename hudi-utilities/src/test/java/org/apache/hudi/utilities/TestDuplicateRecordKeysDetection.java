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

import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.testutils.HoodieSparkClientTestBase;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.common.testutils.RawTripTestPayload.recordToString;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestDuplicateRecordKeysDetection extends HoodieSparkClientTestBase {
  @TempDir
  protected java.nio.file.Path basePath;

  protected HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();

  @Test
  public void testDeleteDuplicates() throws Exception {
    DedupTableJob.spark = this.sparkSession;

    String hudiDir = basePath.toString() + "/hudiDir";
    Map<String, String> writeOptions = new HashMap<>();
    writeOptions.put(DataSourceWriteOptions.TABLE_NAME().key(), "dups");
    writeOptions.put("hoodie.table.name", "dups");
    writeOptions.put(DataSourceWriteOptions.TABLE_TYPE().key(), "MERGE_ON_READ");
    writeOptions.put(DataSourceWriteOptions.RECORDKEY_FIELD().key(), "_row_key");
    writeOptions.put(DataSourceWriteOptions.PRECOMBINE_FIELD().key(), "timestamp");
    writeOptions.put(DataSourceWriteOptions.PARTITIONPATH_FIELD().key(), "partition_path");
    writeOptions.put("hoodie.schema.add.field.ids", "true");

    Dataset<Row> inserts = makeInsertDf("000", 10).cache();
    inserts.write().format("hudi").options(writeOptions)
        .option(DataSourceWriteOptions.OPERATION().key(), WriteOperationType.BULK_INSERT.value())
        .mode(SaveMode.Overwrite)
        .save(hudiDir);
    Dataset<Row> updates = makeUpdateDf("001", 5).cache();
    updates.write().format("hudi").options(writeOptions)
        .option(DataSourceWriteOptions.OPERATION().key(), WriteOperationType.UPSERT.value())
        .mode(SaveMode.Append)
        .save(hudiDir);
    long count = inserts.count();
    Dataset<Row> duplicates = inserts.limit(5).cache();
    duplicates.write().format("hudi").options(writeOptions)
        .option(DataSourceWriteOptions.OPERATION().key(), WriteOperationType.BULK_INSERT.value())
        .mode(SaveMode.Append)
        .save(hudiDir);
    count += duplicates.count();
    Dataset<Row> triples = duplicates.limit(2).cache();
    triples.write().format("hudi").options(writeOptions)
        .option(DataSourceWriteOptions.OPERATION().key(), WriteOperationType.BULK_INSERT.value())
        .mode(SaveMode.Append)
        .save(hudiDir);
    count += triples.count();
    Dataset<Row> triggerCompact = makeInsertDf("002", 1).cache();
    triggerCompact.write().format("hudi").options(writeOptions)
        .option(DataSourceWriteOptions.OPERATION().key(), WriteOperationType.UPSERT.value())
        .option(HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key(), "3")
        .mode(SaveMode.Append)
        .save(hudiDir);

    Configuration conf = new Configuration();
    for (String key : writeOptions.keySet()) {
      conf.set(key, writeOptions.get(key));
    }

    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setBasePath(hudiDir).setConf(conf).build();
    TableSchemaResolver resolver = new TableSchemaResolver(metaClient);

    Schema beforeSchema = resolver.getTableAvroSchema();


    DiffBronzeToSilver.DiffStats diffStats = new DiffBronzeToSilver.DiffStats();

    Dataset<Row> tableData = sparkSession.read().format("hudi").load(hudiDir);
    assertEquals(count + 1, tableData.count());

    Dataset<Row> duplicateRecords = DedupTableJob.findDuplicateRecords(tableData, "timestamp", diffStats, "dups");
    assertEquals(duplicates.count(), duplicateRecords.count());

    DedupTableJob.Config cfg = new DedupTableJob.Config();
    cfg.localMode = true;
    cfg.shouldDeleteDuplicateRecords = true;
    cfg.reIngestDeletedRecords = true;
    cfg.shouldBackupDuplicateRecords = true;
    cfg.baseStagingDir = basePath.toString();

    final LocalDateTime now = LocalDateTime.now();
    DedupTableJob.executeDuplicateHandling(cfg, hudiDir, "", now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm")), writeOptions);
    Dataset<Row> dedupedTable = SparkSession.active().read().format("hudi").load(hudiDir);
    assertEquals(11, dedupedTable.count());
    Dataset<Row> finalOutput = DedupTableJob.findDuplicateRecords(dedupedTable, "timestamp", diffStats, "dups");
    assertEquals(0, finalOutput.count());
    metaClient.reloadActiveTimeline();
    resolver = new TableSchemaResolver(metaClient);
    Schema afterSchema = resolver.getTableAvroSchema();
    assertEquals(beforeSchema, afterSchema);
  }

  protected Dataset<Row> makeInsertDf(String instantTime, Integer n) {
    List<String> records = dataGen.generateInserts(instantTime, n).stream()
        .map(r -> recordToString(r).get()).collect(Collectors.toList());
    JavaRDD<String> rdd = jsc.parallelize(records);
    return sparkSession.read().json(rdd);
  }

  protected Dataset<Row> makeUpdateDf(String instantTime, Integer n) {
    try {
      List<String> records = dataGen.generateUpdates(instantTime, n).stream()
          .map(r -> recordToString(r).get()).collect(Collectors.toList());
      JavaRDD<String> rdd = jsc.parallelize(records);
      return sparkSession.read().json(rdd);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}
