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

package org.apache.hudi.client;

import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.client.embedded.EmbeddedTimelineService;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodieData.HoodieDataCacheKey;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.fs.HoodieWrapperFileSystem;
import org.apache.hudi.common.function.SerializableFunction;
import org.apache.hudi.common.function.SerializablePairFunction;
import org.apache.hudi.common.metrics.Registry;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieDeltaWriteStat;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.index.SparkHoodieIndexFactory;
import org.apache.hudi.internal.schema.utils.SerDeHelper;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.metadata.SparkHoodieBackedTableMetadataWriter;
import org.apache.hudi.metrics.DistributedRegistry;
import org.apache.hudi.table.BulkInsertPartitioner;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.deltacommit.BaseSparkDeltaCommitActionExecutor;
import org.apache.hudi.table.marker.WriteMarkers;
import org.apache.hudi.table.marker.WriteMarkersFactory;
import org.apache.hudi.table.upgrade.SparkUpgradeDowngradeHelper;

import com.codahale.metrics.Timer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static org.apache.hudi.common.model.HoodieCommitMetadata.SCHEMA_KEY;

@SuppressWarnings("checkstyle:LineLength")
public class SparkRDDWriteClient<T> extends
    BaseHoodieWriteClient<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> {

  private static final Logger LOG = LoggerFactory.getLogger(SparkRDDWriteClient.class);

  public SparkRDDWriteClient(HoodieEngineContext context, HoodieWriteConfig clientConfig) {
    this(context, clientConfig, Option.empty());
  }

  @Deprecated
  public SparkRDDWriteClient(HoodieEngineContext context, HoodieWriteConfig writeConfig, boolean rollbackPending) {
    this(context, writeConfig, Option.empty());
  }

  @Deprecated
  public SparkRDDWriteClient(HoodieEngineContext context, HoodieWriteConfig writeConfig, boolean rollbackPending,
                             Option<EmbeddedTimelineService> timelineService) {
    this(context, writeConfig, timelineService);
  }

  public SparkRDDWriteClient(HoodieEngineContext context, HoodieWriteConfig writeConfig,
                             Option<EmbeddedTimelineService> timelineService) {
    super(context, writeConfig, timelineService, SparkUpgradeDowngradeHelper.getInstance());
    this.tableServiceClient = new SparkRDDTableServiceClient<T>(context, writeConfig, getTimelineServer());
  }

  @Override
  protected HoodieIndex createIndex(HoodieWriteConfig writeConfig) {
    return SparkHoodieIndexFactory.createIndex(config);
  }

  /**
   * Complete changes performed at the given instantTime marker with specified action.
   */
  @Override
  public boolean commit(String instantTime, JavaRDD<WriteStatus> writeStatuses, Option<Map<String, String>> extraMetadata,
                        String commitActionType, Map<String, List<String>> partitionToReplacedFileIds,
                        Option<BiConsumer<HoodieTableMetaClient, HoodieCommitMetadata>> extraPreCommitFunc) {
    context.setJobStatus(this.getClass().getSimpleName(), "Committing stats: " + config.getTableName());
    List<HoodieWriteStat> writeStats = writeStatuses.map(WriteStatus::getStat).collect();
    return commitStats(instantTime, HoodieJavaRDD.of(writeStatuses), writeStats, extraMetadata, commitActionType, partitionToReplacedFileIds, extraPreCommitFunc);
  }

  protected void commit(HoodieTable table, String commitActionType, String instantTime, HoodieCommitMetadata metadata,
                        List<HoodieWriteStat> stats, HoodieData<WriteStatus> writeStatuses) throws IOException {
    LOG.info("Committing " + instantTime + " action " + commitActionType);
    HoodieActiveTimeline activeTimeline = table.getActiveTimeline();
    HoodieCommitMetadata fixedCommitMetadata = BaseSparkDeltaCommitActionExecutor.appendMetadataForMissingFiles(table, commitActionType,
        instantTime, metadata, config, context, hadoopConf, this.getClass().getSimpleName());

    addMissingLogFileIfNeeded(table, commitActionType, instantTime, metadata);
    // Finalize write
    finalizeWrite(table, instantTime, stats);
    // do save internal schema to support Implicitly add columns in write process
    if (!fixedCommitMetadata.getExtraMetadata().containsKey(SerDeHelper.LATEST_SCHEMA)
        && fixedCommitMetadata.getExtraMetadata().containsKey(SCHEMA_KEY) && table.getConfig().getSchemaEvolutionEnable()) {
      saveInternalSchema(table, instantTime, fixedCommitMetadata);
    }
    // update Metadata table
    writeTableMetadata(table, instantTime, fixedCommitMetadata, writeStatuses);
    activeTimeline.saveAsComplete(new HoodieInstant(true, commitActionType, instantTime),
        Option.of(fixedCommitMetadata.toJsonString().getBytes(StandardCharsets.UTF_8)));
  }

  /* In spark mor table, any failed spark task may generate log files which are not included in write status.
   * We need to add these to CommitMetadata so that it will be synced to MDT and make MDT has correct file info.
   */
  private HoodieCommitMetadata addMissingLogFileIfNeeded(HoodieTable table, String commitActionType, String instantTime,
                                                         HoodieCommitMetadata commitMetadata) throws IOException {
    if (!table.getMetaClient().getTableConfig().getTableType().equals(HoodieTableType.MERGE_ON_READ)
        || !commitActionType.equals(HoodieActiveTimeline.DELTA_COMMIT_ACTION)) {
      return commitMetadata;
    }

    HoodieCommitMetadata metadata = commitMetadata;
    WriteMarkers markers = WriteMarkersFactory.get(config.getMarkersType(), table, instantTime);
    // if there is log files in this delta commit, we search any invalid log files generated by failed spark task
    boolean hasLogFileInDeltaCommit = metadata.getPartitionToWriteStats()
        .values().stream().flatMap(List::stream)
        .anyMatch(writeStat -> FSUtils.isLogFile(new Path(config.getBasePath(), writeStat.getPath()).getName()));
    if (hasLogFileInDeltaCommit) {
      // get all log files generated by log mark file
      Set<String> logFilesMarkerPath = new HashSet<>(markers.getAppendedLogPaths(context, config.getFinalizeWriteParallelism()));

      // remove valid log files
      for (Map.Entry<String, List<HoodieWriteStat>> partitionAndWriteStats : metadata.getPartitionToWriteStats().entrySet()) {
        for (HoodieWriteStat hoodieWriteStat : partitionAndWriteStats.getValue()) {
          logFilesMarkerPath.remove(hoodieWriteStat.getPath());
        }
      }

      // remaining are log files generated by failed spark task, let's generate write stat for them
      if (logFilesMarkerPath.size() > 0) {
        // populate partition -> map (fileId -> HoodieWriteStat) // we just need one write stat per fileID to fetch some info about the file slice of interest when we want to add a new WriteStat.
        List<Pair<String, Map<String, HoodieWriteStat>>> partitionToFileIdAndWriteStatList = new ArrayList<>();
        for (Map.Entry<String, List<HoodieWriteStat>> partitionAndWriteStats : metadata.getPartitionToWriteStats().entrySet()) {
          String partition = partitionAndWriteStats.getKey();
          Map<String, HoodieWriteStat> fileIdToWriteStat = new HashMap<>();
          partitionAndWriteStats.getValue().forEach(writeStat -> {
            String fileId = writeStat.getFileId();
            if (!fileIdToWriteStat.containsKey(fileId)) {
              fileIdToWriteStat.put(fileId, writeStat);
            }
          });
          partitionToFileIdAndWriteStatList.add(Pair.of(partition, fileIdToWriteStat));
        }

        final Path basePath = new Path(config.getBasePath());
        Map<String, Map<String, List<String>>> partitionToFileIdAndMissingLogFiles = new HashMap<>();
        logFilesMarkerPath
            .stream()
            .forEach(logFilePathStr -> {
              Path logFileFullPath = new Path(config.getBasePath(), logFilePathStr);
              String fileID = FSUtils.getFileId(logFileFullPath.getName());
              String partitionPath = FSUtils.getRelativePartitionPath(basePath, logFileFullPath.getParent());
              if (!partitionToFileIdAndMissingLogFiles.containsKey(partitionPath)) {
                partitionToFileIdAndMissingLogFiles.put(partitionPath, new HashMap<>());
              }
              if (!partitionToFileIdAndMissingLogFiles.get(partitionPath).containsKey(fileID)) {
                partitionToFileIdAndMissingLogFiles.get(partitionPath).put(fileID, new ArrayList<>());
              }
              partitionToFileIdAndMissingLogFiles.get(partitionPath).get(fileID).add(logFilePathStr);
            });

        context.setJobStatus(this.getClass().getSimpleName(), "generate writeStat for missing log files");

        // populate partition -> map (fileId -> List <missing log file>)
        List<Map.Entry<String, Map<String, List<String>>>> missingFilesInfo = partitionToFileIdAndMissingLogFiles.entrySet().stream().collect(Collectors.toList());
        HoodiePairData<String, Map<String, List<String>>> partitionToMissingLogFilesHoodieData = context.parallelize(missingFilesInfo).mapToPair(
            (SerializablePairFunction<Map.Entry<String, Map<String, List<String>>>, String, Map<String, List<String>>>) t -> Pair.of(t.getKey(), t.getValue()));

        // populate partition -> map (fileId -> HoodieWriteStat) // we just need one write stat per fileId to fetch some info about the file slice of interest.
        HoodiePairData<String, Map<String, HoodieWriteStat>> partitionToWriteStatHoodieData = context.parallelize(partitionToFileIdAndWriteStatList).mapToPair(
            (SerializablePairFunction<Pair<String, Map<String, HoodieWriteStat>>, String, Map<String, HoodieWriteStat>>) t -> t);

        SerializableConfiguration serializableConfiguration = new SerializableConfiguration(hadoopConf);

        // lets do left outer join to add write stats for missing log files
        List<Pair<String, List<HoodieWriteStat>>> additionalLogFileWriteStat = partitionToWriteStatHoodieData
            .join(partitionToMissingLogFilesHoodieData)
            .map((SerializableFunction<Pair<String, Pair<Map<String, HoodieWriteStat>, Map<String, List<String>>>>, Pair<String, List<HoodieWriteStat>>>) v1 -> {
              final Path basePathLocal = new Path(config.getBasePath());
              String partitionPath = v1.getKey();
              Map<String, HoodieWriteStat> fileIdToOriginalWriteStat = v1.getValue().getKey();
              Map<String, List<String>> missingFileIdToLogFilesList = v1.getValue().getValue();

              List<HoodieWriteStat> missingWriteStats = new ArrayList();
              List<String> missingLogFilesForPartition = new ArrayList();
              missingFileIdToLogFilesList.values().forEach(entry -> missingLogFilesForPartition.addAll(entry));

              // fetch file sizes for missing log files
              Path fullPartitionPath = new Path(config.getBasePath(), partitionPath);
              FileSystem fileSystem = fullPartitionPath.getFileSystem(serializableConfiguration.get());
              List<Option<FileStatus>> fileStatues = FSUtils.getFileStatusesUnderPartition(fileSystem, fullPartitionPath, missingLogFilesForPartition, true);
              Map<String, List<FileStatus>> fileIdToFileStatuses = new HashMap<>();
              fileStatues.forEach(entry -> {
                if (entry.isPresent()) {
                  FileStatus fileStatus = entry.get();
                  String fileId = FSUtils.getFileId(fileStatus.getPath().getName());
                  if (!fileIdToFileStatuses.containsKey(fileId)) {
                    fileIdToFileStatuses.put(fileId, new ArrayList<>());
                  }
                  fileIdToFileStatuses.get(fileId).add(fileStatus);
                }
              });

              // for each missing log file/fileStatus, add a new DeltaWriteStat.
              fileIdToFileStatuses.forEach((k, v) -> {
                String fileId = k;
                List<FileStatus> missingLogFileFileStatuses = v;
                HoodieDeltaWriteStat existedWriteStat =
                    (HoodieDeltaWriteStat) fileIdToOriginalWriteStat.get(fileId); // are there chances that there won't be any write stat in original list?
                List<String> logFiles = new ArrayList<>(existedWriteStat.getLogFiles());
                missingLogFileFileStatuses.forEach(fileStatus -> {
                  // for every missing file, add a new HoodieDeltaWriteStat
                  HoodieDeltaWriteStat writeStat = new HoodieDeltaWriteStat();
                  HoodieLogFile logFile = new HoodieLogFile(fileStatus);
                  writeStat.setPath(basePathLocal, logFile.getPath());
                  writeStat.setPartitionPath(partitionPath);
                  writeStat.setFileId(fileId);
                  writeStat.setTotalWriteBytes(logFile.getFileSize());
                  writeStat.setFileSizeInBytes(logFile.getFileSize());
                  writeStat.setLogVersion(logFile.getLogVersion());
                  writeStat.setLogFiles(logFiles);
                  writeStat.setBaseFile(existedWriteStat.getBaseFile());
                  writeStat.setPrevCommit(logFile.getBaseCommitTime());
                  missingWriteStats.add(writeStat);
                });
              });
              return Pair.of(partitionPath, missingWriteStats);
            }).collectAsList();

        // add these write stat to commit meta. deltaWriteStat can be empty due to file missing. See code above to address FileNotFoundException
        for (Pair<String, List<HoodieWriteStat>> partitionDeltaStats : additionalLogFileWriteStat) {
          String partitionPath = partitionDeltaStats.getKey();
          partitionDeltaStats.getValue().forEach(ws -> metadata.addWriteStat(partitionPath, ws));
        }
      }
    }
    return metadata;
  }

  @Override
  protected HoodieTable createTable(HoodieWriteConfig config, Configuration hadoopConf) {
    return HoodieSparkTable.create(config, context);
  }

  @Override
  protected HoodieTable createTable(HoodieWriteConfig config, Configuration hadoopConf, HoodieTableMetaClient metaClient) {
    return HoodieSparkTable.create(config, context, metaClient);
  }

  @Override
  public JavaRDD<HoodieRecord<T>> filterExists(JavaRDD<HoodieRecord<T>> hoodieRecords) {
    // Create a Hoodie table which encapsulated the commits and files visible
    HoodieSparkTable<T> table = HoodieSparkTable.create(config, context);
    Timer.Context indexTimer = metrics.getIndexCtx();
    JavaRDD<HoodieRecord<T>> recordsWithLocation = HoodieJavaRDD.getJavaRDD(
        getIndex().tagLocation(HoodieJavaRDD.of(hoodieRecords), context, table));
    metrics.updateIndexMetrics(LOOKUP_STR, metrics.getDurationInMs(indexTimer == null ? 0L : indexTimer.stop()));
    return recordsWithLocation.filter(v1 -> !v1.isCurrentLocationKnown());
  }

  /**
   * Main API to run bootstrap to hudi.
   */
  @Override
  public void bootstrap(Option<Map<String, String>> extraMetadata) {
    initTable(WriteOperationType.UPSERT, Option.ofNullable(HoodieTimeline.METADATA_BOOTSTRAP_INSTANT_TS)).bootstrap(context, extraMetadata);
  }

  @Override
  public JavaRDD<WriteStatus> upsert(JavaRDD<HoodieRecord<T>> records, String instantTime) {
    HoodieTable<T, HoodieData<HoodieRecord<T>>, HoodieData<HoodieKey>, HoodieData<WriteStatus>> table =
        initTable(WriteOperationType.UPSERT, Option.ofNullable(instantTime));
    table.validateUpsertSchema();
    preWrite(instantTime, WriteOperationType.UPSERT, table.getMetaClient());
    HoodieWriteMetadata<HoodieData<WriteStatus>> result = table.upsert(context, instantTime, HoodieJavaRDD.of(records));
    HoodieWriteMetadata<JavaRDD<WriteStatus>> resultRDD = result.clone(HoodieJavaRDD.getJavaRDD(result.getWriteStatuses()));
    if (result.getIndexLookupDuration().isPresent()) {
      metrics.updateIndexMetrics(LOOKUP_STR, result.getIndexLookupDuration().get().toMillis());
    }
    return postWrite(resultRDD, instantTime, table);
  }

  @Override
  public JavaRDD<WriteStatus> upsertPreppedRecords(JavaRDD<HoodieRecord<T>> preppedRecords, String instantTime) {
    HoodieTable<T, HoodieData<HoodieRecord<T>>, HoodieData<HoodieKey>, HoodieData<WriteStatus>> table =
        initTable(WriteOperationType.UPSERT_PREPPED, Option.ofNullable(instantTime));
    table.validateUpsertSchema();
    preWrite(instantTime, WriteOperationType.UPSERT_PREPPED, table.getMetaClient());
    HoodieWriteMetadata<HoodieData<WriteStatus>> result = table.upsertPrepped(context, instantTime, HoodieJavaRDD.of(preppedRecords));
    HoodieWriteMetadata<JavaRDD<WriteStatus>> resultRDD = result.clone(HoodieJavaRDD.getJavaRDD(result.getWriteStatuses()));
    return postWrite(resultRDD, instantTime, table);
  }

  @Override
  public JavaRDD<WriteStatus> insert(JavaRDD<HoodieRecord<T>> records, String instantTime) {
    HoodieTable<T, HoodieData<HoodieRecord<T>>, HoodieData<HoodieKey>, HoodieData<WriteStatus>> table =
        initTable(WriteOperationType.INSERT, Option.ofNullable(instantTime));
    table.validateInsertSchema();
    preWrite(instantTime, WriteOperationType.INSERT, table.getMetaClient());
    HoodieWriteMetadata<HoodieData<WriteStatus>> result = table.insert(context, instantTime, HoodieJavaRDD.of(records));
    HoodieWriteMetadata<JavaRDD<WriteStatus>> resultRDD = result.clone(HoodieJavaRDD.getJavaRDD(result.getWriteStatuses()));
    return postWrite(resultRDD, instantTime, table);
  }

  @Override
  public JavaRDD<WriteStatus> insertPreppedRecords(JavaRDD<HoodieRecord<T>> preppedRecords, String instantTime) {
    HoodieTable<T, HoodieData<HoodieRecord<T>>, HoodieData<HoodieKey>, HoodieData<WriteStatus>> table =
        initTable(WriteOperationType.INSERT_PREPPED, Option.ofNullable(instantTime));
    table.validateInsertSchema();
    preWrite(instantTime, WriteOperationType.INSERT_PREPPED, table.getMetaClient());
    HoodieWriteMetadata<HoodieData<WriteStatus>> result = table.insertPrepped(context, instantTime, HoodieJavaRDD.of(preppedRecords));
    HoodieWriteMetadata<JavaRDD<WriteStatus>> resultRDD = result.clone(HoodieJavaRDD.getJavaRDD(result.getWriteStatuses()));
    return postWrite(resultRDD, instantTime, table);
  }

  /**
   * Removes all existing records from the partitions affected and inserts the given HoodieRecords, into the table.
   *
   * @param records HoodieRecords to insert
   * @param instantTime Instant time of the commit
   * @return JavaRDD[WriteStatus] - RDD of WriteStatus to inspect errors and counts
   */
  public HoodieWriteResult insertOverwrite(JavaRDD<HoodieRecord<T>> records, final String instantTime) {
    HoodieTable<T, HoodieData<HoodieRecord<T>>, HoodieData<HoodieKey>, HoodieData<WriteStatus>> table = initTable(WriteOperationType.INSERT_OVERWRITE, Option.ofNullable(instantTime));
    table.validateInsertSchema();
    preWrite(instantTime, WriteOperationType.INSERT_OVERWRITE, table.getMetaClient());
    HoodieWriteMetadata<HoodieData<WriteStatus>> result = table.insertOverwrite(context, instantTime, HoodieJavaRDD.of(records));
    HoodieWriteMetadata<JavaRDD<WriteStatus>> resultRDD = result.clone(HoodieJavaRDD.getJavaRDD(result.getWriteStatuses()));
    return new HoodieWriteResult(postWrite(resultRDD, instantTime, table), result.getPartitionToReplaceFileIds());
  }

  /**
   * Removes all existing records of the Hoodie table and inserts the given HoodieRecords, into the table.
   *
   * @param records HoodieRecords to insert
   * @param instantTime Instant time of the commit
   * @return JavaRDD[WriteStatus] - RDD of WriteStatus to inspect errors and counts
   */
  public HoodieWriteResult insertOverwriteTable(JavaRDD<HoodieRecord<T>> records, final String instantTime) {
    HoodieTable<T, HoodieData<HoodieRecord<T>>, HoodieData<HoodieKey>, HoodieData<WriteStatus>> table = initTable(WriteOperationType.INSERT_OVERWRITE_TABLE, Option.ofNullable(instantTime));
    table.validateInsertSchema();
    preWrite(instantTime, WriteOperationType.INSERT_OVERWRITE_TABLE, table.getMetaClient());
    HoodieWriteMetadata<HoodieData<WriteStatus>> result = table.insertOverwriteTable(context, instantTime, HoodieJavaRDD.of(records));
    HoodieWriteMetadata<JavaRDD<WriteStatus>> resultRDD = result.clone(HoodieJavaRDD.getJavaRDD(result.getWriteStatuses()));
    return new HoodieWriteResult(postWrite(resultRDD, instantTime, table), result.getPartitionToReplaceFileIds());
  }

  @Override
  public JavaRDD<WriteStatus> bulkInsert(JavaRDD<HoodieRecord<T>> records, String instantTime) {
    return bulkInsert(records, instantTime, Option.empty());
  }

  @Override
  public JavaRDD<WriteStatus> bulkInsert(JavaRDD<HoodieRecord<T>> records, String instantTime, Option<BulkInsertPartitioner> userDefinedBulkInsertPartitioner) {
    HoodieTable<T, HoodieData<HoodieRecord<T>>, HoodieData<HoodieKey>, HoodieData<WriteStatus>> table =
        initTable(WriteOperationType.BULK_INSERT, Option.ofNullable(instantTime));
    table.validateInsertSchema();
    preWrite(instantTime, WriteOperationType.BULK_INSERT, table.getMetaClient());
    HoodieWriteMetadata<HoodieData<WriteStatus>> result = table.bulkInsert(context, instantTime, HoodieJavaRDD.of(records), userDefinedBulkInsertPartitioner);
    HoodieWriteMetadata<JavaRDD<WriteStatus>> resultRDD = result.clone(HoodieJavaRDD.getJavaRDD(result.getWriteStatuses()));
    return postWrite(resultRDD, instantTime, table);
  }

  @Override
  public JavaRDD<WriteStatus> bulkInsertPreppedRecords(JavaRDD<HoodieRecord<T>> preppedRecords, String instantTime, Option<BulkInsertPartitioner> bulkInsertPartitioner) {
    HoodieTable<T, HoodieData<HoodieRecord<T>>, HoodieData<HoodieKey>, HoodieData<WriteStatus>> table =
        initTable(WriteOperationType.BULK_INSERT_PREPPED, Option.ofNullable(instantTime));
    table.validateInsertSchema();
    preWrite(instantTime, WriteOperationType.BULK_INSERT_PREPPED, table.getMetaClient());
    HoodieWriteMetadata<HoodieData<WriteStatus>> result = table.bulkInsertPrepped(context, instantTime, HoodieJavaRDD.of(preppedRecords), bulkInsertPartitioner);
    HoodieWriteMetadata<JavaRDD<WriteStatus>> resultRDD = result.clone(HoodieJavaRDD.getJavaRDD(result.getWriteStatuses()));
    return postWrite(resultRDD, instantTime, table);
  }

  @Override
  public JavaRDD<WriteStatus> delete(JavaRDD<HoodieKey> keys, String instantTime) {
    HoodieTable<T, HoodieData<HoodieRecord<T>>, HoodieData<HoodieKey>, HoodieData<WriteStatus>> table = initTable(WriteOperationType.DELETE, Option.ofNullable(instantTime));
    preWrite(instantTime, WriteOperationType.DELETE, table.getMetaClient());
    HoodieWriteMetadata<HoodieData<WriteStatus>> result = table.delete(context, instantTime, HoodieJavaRDD.of(keys));
    HoodieWriteMetadata<JavaRDD<WriteStatus>> resultRDD = result.clone(HoodieJavaRDD.getJavaRDD(result.getWriteStatuses()));
    return postWrite(resultRDD, instantTime, table);
  }

  @Override
  public JavaRDD<WriteStatus> deletePrepped(JavaRDD<HoodieRecord<T>> preppedRecord, String instantTime) {
    HoodieTable<T, HoodieData<HoodieRecord<T>>, HoodieData<HoodieKey>, HoodieData<WriteStatus>> table = initTable(WriteOperationType.DELETE_PREPPED, Option.ofNullable(instantTime));
    preWrite(instantTime, WriteOperationType.DELETE_PREPPED, table.getMetaClient());
    HoodieWriteMetadata<HoodieData<WriteStatus>> result = table.deletePrepped(context, instantTime, HoodieJavaRDD.of(preppedRecord));
    HoodieWriteMetadata<JavaRDD<WriteStatus>> resultRDD = result.clone(HoodieJavaRDD.getJavaRDD(result.getWriteStatuses()));
    return postWrite(resultRDD, instantTime, table);
  }

  public HoodieWriteResult deletePartitions(List<String> partitions, String instantTime) {
    HoodieTable<T, HoodieData<HoodieRecord<T>>, HoodieData<HoodieKey>, HoodieData<WriteStatus>> table = initTable(WriteOperationType.DELETE_PARTITION, Option.ofNullable(instantTime));
    preWrite(instantTime, WriteOperationType.DELETE_PARTITION, table.getMetaClient());
    HoodieWriteMetadata<HoodieData<WriteStatus>> result = table.deletePartitions(context, instantTime, partitions);
    HoodieWriteMetadata<JavaRDD<WriteStatus>> resultRDD = result.clone(HoodieJavaRDD.getJavaRDD(result.getWriteStatuses()));
    return new HoodieWriteResult(postWrite(resultRDD, instantTime, table), result.getPartitionToReplaceFileIds());
  }

  @Override
  protected void initMetadataTable(Option<String> instantTime) {
    // Initialize Metadata Table to make sure it's bootstrapped _before_ the operation,
    // if it didn't exist before
    // See https://issues.apache.org/jira/browse/HUDI-3343 for more details
    initializeMetadataTable(instantTime);
  }

  /**
   * Initialize the metadata table if needed. Creating the metadata table writer
   * will trigger the initial bootstrapping from the data table.
   *
   * @param inFlightInstantTimestamp - The in-flight action responsible for the metadata table initialization
   */
  private void initializeMetadataTable(Option<String> inFlightInstantTimestamp) {
    if (!config.isMetadataTableEnabled()) {
      return;
    }

    try (HoodieTableMetadataWriter writer = SparkHoodieBackedTableMetadataWriter.create(context.getHadoopConf().get(), config,
        context, inFlightInstantTimestamp)) {
      if (writer.isInitialized()) {
        writer.performTableServices(inFlightInstantTimestamp);
      }
    } catch (Exception e) {
      throw new HoodieException("Failed to instantiate Metadata table ", e);
    }
  }

  @Override
  protected void initWrapperFSMetrics() {
    if (config.isMetricsOn()) {
      Registry registry;
      Registry registryMeta;
      JavaSparkContext jsc = ((HoodieSparkEngineContext) context).getJavaSparkContext();

      if (config.isExecutorMetricsEnabled()) {
        // Create a distributed registry for HoodieWrapperFileSystem
        registry = Registry.getRegistry(HoodieWrapperFileSystem.class.getSimpleName(),
            DistributedRegistry.class.getName());
        ((DistributedRegistry) registry).register(jsc);
        registryMeta = Registry.getRegistry(HoodieWrapperFileSystem.class.getSimpleName() + "MetaFolder",
            DistributedRegistry.class.getName());
        ((DistributedRegistry) registryMeta).register(jsc);
      } else {
        registry = Registry.getRegistry(HoodieWrapperFileSystem.class.getSimpleName());
        registryMeta = Registry.getRegistry(HoodieWrapperFileSystem.class.getSimpleName() + "MetaFolder");
      }

      HoodieWrapperFileSystem.setMetricsRegistry(registry, registryMeta);
    }
  }

  @Override
  protected void releaseResources(String instantTime) {
    // If we do not explicitly release the resource, spark will automatically manage the resource and clean it up automatically
    // see: https://spark.apache.org/docs/latest/rdd-programming-guide.html#removing-data
    if (config.areReleaseResourceEnabled()) {
      HoodieSparkEngineContext sparkEngineContext = (HoodieSparkEngineContext) context;
      Map<Integer, JavaRDD<?>> allCachedRdds = sparkEngineContext.getJavaSparkContext().getPersistentRDDs();
      List<Integer> allDataIds = new ArrayList<>(sparkEngineContext.removeCachedDataIds(HoodieDataCacheKey.of(basePath, instantTime)));
      if (config.isMetadataTableEnabled()) {
        String metadataTableBasePath = HoodieTableMetadata.getMetadataTableBasePath(basePath);
        allDataIds.addAll(sparkEngineContext.removeCachedDataIds(HoodieDataCacheKey.of(metadataTableBasePath, instantTime)));
      }
      for (int id : allDataIds) {
        if (allCachedRdds.containsKey(id)) {
          allCachedRdds.get(id).unpersist();
        }
      }
    }
  }
}
