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
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import io.prometheus.client.CollectorRegistry;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import scala.collection.JavaConverters;

import static org.apache.hudi.common.model.HoodieRecord.COMMIT_SEQNO_METADATA_FIELD;
import static org.apache.hudi.common.model.HoodieRecord.COMMIT_TIME_METADATA_FIELD;
import static org.apache.hudi.common.model.HoodieRecord.FILENAME_METADATA_FIELD;
import static org.apache.hudi.common.model.HoodieRecord.PARTITION_PATH_METADATA_FIELD;
import static org.apache.hudi.common.model.HoodieRecord.RECORD_KEY_METADATA_FIELD;
import static org.apache.hudi.common.util.StringUtils.nonEmpty;
import static org.apache.hudi.common.util.ValidationUtils.checkArgument;
import static org.apache.hudi.config.HoodieWriteConfig.TBL_NAME;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.countDistinct;
import static org.apache.spark.sql.functions.max;

public class DedupTableJob {

  private static final Logger LOG = LoggerFactory.getLogger(DiffBronzeToSilver.class);

  private static final CollectorRegistry REGISTRY = new CollectorRegistry();
  //private static final List<TableIngestionState> ACTIVE_TABLE_STATES = Arrays.asList(TABLE_INGESTION_STATE_RUNNING, TABLE_INGESTION_STATE_FAILED, TABLE_INGESTION_STATE_PENDING);

  public static SparkSession spark = null;
  private static String jobName;

  //private static Gauge duplicateCountGauge;
  //private static PrometheusMetricExporter prometheusMetricExporter;

  public static class Config implements Serializable {

    @Parameter(names = {"--table-path"})
    public String tablePath;

    @Parameter(names = {"--table-props-path"})
    public String tablePropsPath;

    @Parameter(names = {"--backup-duplicate-records"})
    public Boolean shouldBackupDuplicateRecords = true;

    @Parameter(names = {"--use-data-from-backup-for-duplicates"})
    public Boolean useDataFromBackupForDuplicates = false;

    @Parameter(names = {"--delete-duplicate-records"})
    public Boolean shouldDeleteDuplicateRecords = false;

    @Parameter(names = {"--re-ingest-deleted-records"})
    public Boolean reIngestDeletedRecords = false;

    @Parameter(names = {"--local-mode"})
    public Boolean localMode = false;

    @Parameter(names = {"--base-staging-dir"})
    public String baseStagingDir;

    @Parameter(names = {"--backup-dir-for-dups"})
    public String backupDirForDups;

    @Parameter(names = {"--publish-metrics"})
    public Boolean shouldPublishMetrics = false;

    @Parameter(names = {"--help", "-h"}, help = true)
    public Boolean help = false;
  }

  public static void main(String[] args) throws IOException {
    Config cfg = new Config();
    JCommander cmd = new JCommander(cfg, null, args);
    if (cfg.help) {
      cmd.usage();
    }

    final LocalDateTime now = LocalDateTime.now();
    final String currentHour = now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm"));
    jobName = "diff-bronze-to-silver";
    String sparkAppName = jobName + "-" + currentHour;
    spark = SparkSession.builder()
        .appName(sparkAppName)
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
        .getOrCreate();

    LOG.info(sparkAppName + " started.");
    if (nonEmpty(cfg.tablePath)) {
      checkArgument(nonEmpty(cfg.tablePath), "Table path should not be empty");
      checkArgument(nonEmpty(cfg.tablePropsPath), "Table props path should not be empty");
      try {
        executeDuplicateHandling(cfg, cfg.tablePath, cfg.tablePropsPath, currentHour);
      } catch (Exception e) {
        String tableName = cfg.tablePath.replaceAll(".*/", "");
        LOG.error(String.format("For-table %s diff-failed.", tableName), e);
      }
    } else {
      throw new IOException("Table path needs to be set");
    }
    spark.stop();
  }

  private static void executeDuplicateHandling(Config cfg, String tablePath, String propsPath, String curTimeSuffix) throws IOException {
    executeDuplicateHandling(cfg, tablePath, propsPath, curTimeSuffix, null);
  }

  public static void executeDuplicateHandling(Config cfg, String tablePath, String tablePropsPath, String curTimeSuffix, Map<String, String> props) throws IOException {
    Schema beginningSchema = null;
    Schema afterDeleteSchema = null;
    Schema finalSchema = null;
    Map<String, String> writerProps;
    LOG.warn("For-table-path " + tablePath);
    if (!cfg.localMode) {
      validateParameters(tablePath, tablePropsPath);
      writerProps = getSilverWriterPropsMap(spark.sparkContext().hadoopConfiguration(), tablePropsPath);
    } else {
      writerProps = props;
    }
    // validate configs
    String tableName = writerProps.get(TBL_NAME.key());
    String metadataEnabled = writerProps.get(HoodieMetadataConfig.ENABLE.key());
    if (StringUtils.isNullOrEmpty(metadataEnabled) || !metadataEnabled.equals("false")) {
      LOG.warn(String.format("For-table %s metadata-table-enabled", tableName));
    }

    String recordKey = writerProps.get(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key());
    String precombineKey = writerProps.get(HoodieWriteConfig.PRECOMBINE_FIELD_NAME.key());
    String partitionPath = writerProps.get(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key());
    // find duplicates
    final Dataset<Row> originalDataset = spark.read().format("hudi")
        .option(HoodieMetadataConfig.ENABLE.key(), "false")
        .load(tablePath);
    final DiffBronzeToSilver.DiffStats stats = new DiffBronzeToSilver.DiffStats();
    List<String> colsToJoin = Arrays.asList(PARTITION_PATH_METADATA_FIELD, RECORD_KEY_METADATA_FIELD);
    boolean hasDup = false;
    Dataset<Row> duplicatedRows = null;
    if (cfg.useDataFromBackupForDuplicates) {
      hasDup = true;
      if (StringUtils.isNullOrEmpty(cfg.backupDirForDups)) {
        throw new IllegalArgumentException("Please set value for --backup-dir-for-dups");
      }
      stats.dedupBackupPath = cfg.backupDirForDups;
      LOG.info(String.format("For-table %s start reading duplicate records from back up %s ", tableName, cfg.backupDirForDups));
      duplicatedRows = spark.read().format("parquet").load(cfg.backupDirForDups);
    } else {
      Dataset<Row> duplicateRecordKeys = findDuplicateRecords(originalDataset, precombineKey, stats, tableName);
      hasDup = stats.numDups > 0;
      if (hasDup) {
        duplicatedRows = duplicateRecordKeys.join(spark.read().format("hudi").load(tablePath), JavaConverters.asScalaBuffer(colsToJoin).toSeq());

        if (cfg.shouldPublishMetrics) {
          LOG.warn(String.format("For-table %s found-duplicates with stats: %s", tableName, stats));
          //duplicateCountGauge.set(stats.numDups);
          //prometheusMetricExporter.pushAllToPrometheus(REGISTRY, jobName, Collections.singletonMap("table_name", tableName));
        }

        if (cfg.shouldBackupDuplicateRecords) {
          LOG.info(String.format("For-table %s start backing up delete records", tableName));
          final String dedupBackupPath = String.format("%s/%s/delete_backup_%s", cfg.baseStagingDir, tableName, curTimeSuffix);
          LOG.info(String.format("For-table %s start backing up delete records to %s", tableName, dedupBackupPath));
          stats.dedupBackupPath = dedupBackupPath;
          duplicatedRows.persist(StorageLevel.DISK_ONLY());
          duplicatedRows.write().format("parquet").mode(SaveMode.Overwrite).save(dedupBackupPath);
          LOG.info(String.format("For-table %s finished backing up delete records ", tableName));
        }
      }
    }

    Configuration conf = new Configuration();
    for (String key : writerProps.keySet()) {
      conf.set(key, writerProps.get(key));
    }

    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setBasePath(tablePath).setConf(conf).build();
    beginningSchema = getCurrentTableSchema(metaClient);

    LOG.info("For-table-stat " + tableName + " - Stats before dedup: " + stats);
    if (hasDup && cfg.shouldDeleteDuplicateRecords) {
      Dataset<Row> deletionDf = duplicatedRows.select(partitionPath.replace(":simple", ""), recordKey, precombineKey);
      LOG.info(String.format("For-table %s start deleting records count= %d ", tableName, deletionDf.count()));

      deletionDf // hudi should auto de-dup multiple versions of the record
          .write().format("hudi").options(writerProps)
          .option(DataSourceWriteOptions.OPERATION().key(), "delete")
          .option(HoodieMetadataConfig.ENABLE.key(), "false")
          .option(HoodieCompactionConfig.INLINE_COMPACT.key(), "true")
          .option(HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key(), "1")
          .mode(SaveMode.Append)
          .save(tablePath);
      LOG.info(String.format("For-table %s finished deleting records ", tableName));

      // verifying all records are deleted as intended
      spark.sqlContext().clearCache();
      long postDeleteDuplicates = spark.read().format("hudi")
          .option(HoodieMetadataConfig.ENABLE.key(), "false")
          .load(tablePath).groupBy(PARTITION_PATH_METADATA_FIELD, RECORD_KEY_METADATA_FIELD)
          .agg(count("*").as("ct"))
          .filter("ct > 1").count();
      if (postDeleteDuplicates == 0) {
        LOG.info(String.format("For-table %s Validated that all duplicated records are deleted ", tableName));
      } else {
        LOG.error(String.format("For-table %s Validation FAILED to delete all duplicated records ", tableName));
        throw new IOException("Validation FAILED to delete all duplicated records for table " + tableName);
      }

      afterDeleteSchema = getCurrentTableSchema(metaClient);
      if (afterDeleteSchema != null && beginningSchema != null) {
        if (!afterDeleteSchema.equals(beginningSchema)) {
          LOG.error(String.format("Schema before the delete: %s, Schema after the delete: %s", beginningSchema, afterDeleteSchema));
          throw new IOException("Table schema has been messed up. Roll back the table.");
        }
        LOG.info("Schema was not impacted by the delete");
      }
    }

    if (cfg.reIngestDeletedRecords) {
      LOG.info(String.format("For-table %s start re-ingesting deleted records", tableName));
      // hudi should be able to dedup based on precombine. so, we don't need to de-dup from our end.
      duplicatedRows = spark.read().format("parquet").load(stats.dedupBackupPath);
      duplicatedRows.drop(PARTITION_PATH_METADATA_FIELD, RECORD_KEY_METADATA_FIELD, COMMIT_TIME_METADATA_FIELD, COMMIT_SEQNO_METADATA_FIELD, FILENAME_METADATA_FIELD)
          .write().format("hudi").options(writerProps)
          .option(DataSourceWriteOptions.OPERATION().key(), "upsert")
          .option(HoodieMetadataConfig.ENABLE.key(), "false")
          .option("hoodie.combine.before.insert", "true")
          .option("hoodie.datasource.write.row.writer.enable", "false")
          .mode(SaveMode.Append)
          .save(tablePath);
      LOG.info(String.format("For-table %s finished deleting records ", tableName));

      // finally validate that no duplicates are found
      // verifying all records are deleted as intended
      spark.sqlContext().clearCache();
      long postWriteDuplicates = spark.read().format("hudi")
          .option(HoodieMetadataConfig.ENABLE.key(), "false")
          .load(tablePath).groupBy(PARTITION_PATH_METADATA_FIELD, RECORD_KEY_METADATA_FIELD)
          .agg(count("*").as("ct"))
          .filter("ct > 1").count();

      if (postWriteDuplicates == 0) {
        LOG.info(String.format("For-table %s Validated that all duplicated records are deleted ", tableName));
      } else {
        LOG.error(String.format("For-table %s found duplicates after mitigation. Total dups found %s ", tableName, postWriteDuplicates));
        throw new IOException(String.format("For-table %s found duplicates after mitigation. Total dups found %s ", tableName, postWriteDuplicates));
      }

      if (beginningSchema == null) {
        beginningSchema = afterDeleteSchema;
      }


      if (beginningSchema != null) {

        finalSchema = getCurrentTableSchema(metaClient);
        if (!finalSchema.equals(beginningSchema)) {
          LOG.error(String.format("Schema before the write: %s, Schema after the write: %s", beginningSchema, afterDeleteSchema));
          throw new IOException("Table schema has been messed up. Roll back the table.");
        }
        LOG.info("Schema was not impacted by the write");
      }
    }

    LOG.warn("For-table-stat " + tableName + " - " + stats);

    spark.sparkContext().getPersistentRDDs().values().foreach(rdd -> rdd.unpersist(true));
    LOG.info("For-table " + tableName + " job ended.");
  }

  private static Schema getCurrentTableSchema(HoodieTableMetaClient metaClient) throws IOException {
    metaClient.reloadActiveTimeline();
    TableSchemaResolver resolver = new TableSchemaResolver(metaClient);
    try {
      return resolver.getTableAvroSchema();
    } catch (Exception e) {
      LOG.error("unable to get the table schema", e);
      throw new IOException("unable to get the table schema", e);
    }
  }

  public static void validateParameters(String silverPath, String silverPropsPath) {
    checkArgument(nonEmpty(silverPath));
    checkArgument(nonEmpty(silverPropsPath) && silverPropsPath.endsWith("source_specific.properties"));
  }

  public static Dataset<Row> findDuplicateRecords(
      Dataset<Row> hudiDataset,
      String precombineKey,
      DiffBronzeToSilver.DiffStats stats, String tableName) {
    LOG.info(String.format("For-table %s Start finding duplicate records ", tableName));
    // _hoodie_record_key, _hoodie_partition_path, precombineKey
    final Dataset<Row> hudiKeyPartitionAndOrder = hudiDataset.select(RECORD_KEY_METADATA_FIELD, PARTITION_PATH_METADATA_FIELD, precombineKey);
    hudiKeyPartitionAndOrder.persist(StorageLevel.DISK_ONLY());
    stats.numTotalInSilver = hudiKeyPartitionAndOrder.count();

    // _hoodie_record_key, _hoodie_partition_path, precombineKey
    final Dataset<Row> dupHoodieKeys = hudiKeyPartitionAndOrder
        .groupBy(PARTITION_PATH_METADATA_FIELD, RECORD_KEY_METADATA_FIELD)
        .agg(count("*").as("ct"))
        .filter("ct > 1")
        .select(PARTITION_PATH_METADATA_FIELD, RECORD_KEY_METADATA_FIELD);
    dupHoodieKeys.persist(StorageLevel.DISK_ONLY());
    stats.numDups = dupHoodieKeys.count();
    calculateStats(hudiKeyPartitionAndOrder, stats, precombineKey);
    LOG.info(String.format("For-table %s Finished finding duplicate records ", tableName));
    return dupHoodieKeys;
  }

  private static void calculateStats(Dataset<Row> hudiDataset, DiffBronzeToSilver.DiffStats stats, String precombineKey) {
    if (stats.numDups == 0) {
      return;
    }
    Row maxCtRow = hudiDataset
        .groupBy(PARTITION_PATH_METADATA_FIELD, RECORD_KEY_METADATA_FIELD)
        .agg(count("*").as("ct"))
        .agg(max(col("ct")).as("max_ct"))
        .head();
    stats.maxDupCount = maxCtRow.isNullAt(0) ? 0 : maxCtRow.getLong(0);
    stats.numDupsWithDifferentOrder = hudiDataset
        .groupBy(PARTITION_PATH_METADATA_FIELD, RECORD_KEY_METADATA_FIELD)
        .agg(countDistinct(precombineKey).as("ct_order"))
        .filter("ct_order > 1")
        .count();
  }

  public static Map<String, String> getSilverWriterPropsMap(Configuration hadoopConf, String silverPropsPath) {
    TypedProperties originalProps = UtilHelpers.readConfig(hadoopConf, new Path(silverPropsPath), Collections.emptyList()).getProps();
    Map<String, String> propsMap = new HashMap<>();

    //load in all combined props
    for (Object keyObj : originalProps.keySet()) {
      String key = keyObj.toString();
      propsMap.put(key, originalProps.getString(key));
    }
    //disable all table services
    propsMap.put(HoodieCompactionConfig.INLINE_COMPACT.key(), "false");
    propsMap.put(HoodieCompactionConfig.SCHEDULE_INLINE_COMPACT.key(), "false");
    propsMap.put(HoodieCompactionConfig.INLINE_LOG_COMPACT.key(), "false");
    propsMap.put(HoodieClusteringConfig.INLINE_CLUSTERING.key(), "false");
    propsMap.put(HoodieClusteringConfig.SCHEDULE_INLINE_CLUSTERING.key(), "false");
    propsMap.put(HoodieCleanConfig.AUTO_CLEAN.key(), "false");
    propsMap.put(HoodieArchivalConfig.AUTO_ARCHIVE.key(), "false");
    propsMap.put(HoodieArchivalConfig.ARCHIVE_BEYOND_SAVEPOINT.key(), "false");
    LOG.warn("Relevant silver writer props:\n"
        + propsMap.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue()).collect(Collectors.joining("\n")));
    return propsMap;
  }
}
