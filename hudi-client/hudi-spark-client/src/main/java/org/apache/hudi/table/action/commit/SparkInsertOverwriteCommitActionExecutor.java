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

package org.apache.hudi.table.action.commit;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieInternalConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaPairRDD;
import org.apache.hudi.exception.MetadataNotFoundException;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.io.storage.HoodieFileReaderFactory;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.WorkloadProfile;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.Partitioner;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

public class SparkInsertOverwriteCommitActionExecutor<T>
    extends BaseSparkCommitActionExecutor<T> {

  private final HoodieData<HoodieRecord<T>> inputRecordsRDD;

  public SparkInsertOverwriteCommitActionExecutor(HoodieEngineContext context,
                                                  HoodieWriteConfig config, HoodieTable table,
                                                  String instantTime, HoodieData<HoodieRecord<T>> inputRecordsRDD) {
    this(context, config, table, instantTime, inputRecordsRDD, WriteOperationType.INSERT_OVERWRITE);
  }

  public SparkInsertOverwriteCommitActionExecutor(HoodieEngineContext context,
                                                  HoodieWriteConfig config, HoodieTable table,
                                                  String instantTime, HoodieData<HoodieRecord<T>> inputRecordsRDD,
                                                  WriteOperationType writeOperationType) {
    super(context, config, table, instantTime, writeOperationType);
    this.inputRecordsRDD = inputRecordsRDD;
  }

  public SparkInsertOverwriteCommitActionExecutor(HoodieEngineContext context,
                                                  HoodieWriteConfig config, HoodieTable table,
                                                  String instantTime, HoodieData<HoodieRecord<T>> inputRecordsRDD,
                                                  WriteOperationType writeOperationType,
                                                  Option<Map<String, String>> extraMetadata) {
    super(context, config, table, instantTime, writeOperationType, extraMetadata);
    this.inputRecordsRDD = inputRecordsRDD;
  }

  @Override
  public HoodieWriteMetadata<HoodieData<WriteStatus>> execute() {
    return HoodieWriteHelper.newInstance().write(instantTime, inputRecordsRDD, context, table,
        config.shouldCombineBeforeInsert(), config.getInsertShuffleParallelism(), this, operationType);
  }

  @Override
  protected Partitioner getPartitioner(WorkloadProfile profile) {
    return table.getStorageLayout().layoutPartitionerClass()
        .map(c -> getLayoutPartitioner(profile, c))
        .orElse(new SparkInsertOverwritePartitioner(profile, context, table, config));
  }

  @Override
  protected String getCommitActionType() {
    return HoodieTimeline.REPLACE_COMMIT_ACTION;
  }

  @Override
  protected Map<String, List<String>> getPartitionToReplacedFileIds(HoodieWriteMetadata<HoodieData<WriteStatus>> writeMetadata) {
    String staticOverwritePartition = config.getStringOrDefault(HoodieInternalConfig.STATIC_OVERWRITE_PARTITION_PATHS);
    if (StringUtils.nonEmpty(staticOverwritePartition)) {
      // static insert overwrite partitions
      List<String> partitionPaths = Arrays.asList(staticOverwritePartition.split(","));
      context.setJobStatus(this.getClass().getSimpleName(), "Getting ExistingFileIds of matching static partitions");
      return HoodieJavaPairRDD.getJavaPairRDD(context.parallelize(partitionPaths, partitionPaths.size()).mapToPair(
          partitionPath -> Pair.of(partitionPath, getAllExistingFileIds(partitionPath)))).collectAsMap();
    } else {
      // dynamic insert overwrite partitions
      return HoodieJavaPairRDD.getJavaPairRDD(writeMetadata.getWriteStatuses().map(status -> status.getStat().getPartitionPath()).distinct().mapToPair(partitionPath ->
          Pair.of(partitionPath, getAllExistingFileIds(partitionPath)))).collectAsMap();
    }
  }

  protected List<String> getAllExistingFileIds(String partitionPath) {
    // because new commit is not complete. it is safe to mark all existing file Ids as old files
    return table.getSliceView().getLatestFileSlices(partitionPath).map(FileSlice::getFileId).distinct().collect(Collectors.toList());
  }

  @Override
  protected Iterator<List<WriteStatus>> handleInsertPartition(String instantTime, Integer partition, Iterator recordItr, Partitioner partitioner) {
    SparkHoodiePartitioner upsertPartitioner = (SparkHoodiePartitioner) partitioner;
    BucketInfo binfo = upsertPartitioner.getBucketInfo(partition);
    BucketType btype = binfo.bucketType;
    switch (btype) {
      case INSERT:
        return handleInsert(binfo.fileIdPrefix, recordItr);
      default:
        throw new AssertionError("Expect INSERT bucketType for insert overwrite, please correct the logical of " + partitioner.getClass().getName());
    }
  }

  protected List<Option<HoodieBaseFile>> getAllExistingBaseFiles(String partitionPath) {
    return table
        .getSliceView()
        .getAllFileSlices(partitionPath)
        .map(fg -> fg.getBaseFile()).distinct().collect(Collectors.toList());
  }

  protected WriteStatus generateWriteStatus(HoodieBaseFile baseFile, String fileId, String partition,
                                            String baseLocation, Path filePath) {
    try {
      HoodieFileReader reader = HoodieFileReaderFactory.getReaderFactory(HoodieRecord.HoodieRecordType.AVRO)
          .getFileReader(table.getHadoopConf(), filePath);
      long totalRecords = reader.getTotalRecords();
      long fileSizeInBytes = baseFile.getFileSize();
      WriteStatus ws = new WriteStatus(false, config.getWriteStatusFailureFraction());
      ws.setFileId(fileId);
      ws.setPartitionPath(partition);
      ws.setTotalRecords(totalRecords);
      HoodieWriteStat stat = new HoodieWriteStat();
      stat.setNumWrites(totalRecords);
      stat.setNumInserts(totalRecords);
      stat.setFileId(ws.getFileId());
      stat.setTotalWriteBytes(fileSizeInBytes);
      stat.setFileSizeInBytes(fileSizeInBytes);
      stat.setPartitionPath(ws.getPartitionPath());
      stat.setPrevCommit(HoodieWriteStat.NULL_COMMIT);
      stat.setFileId(ws.getFileId());
      stat.setPath(FSUtils.getRelativePartitionPath(new Path(baseLocation), filePath));
      ws.setStat(stat);
      return ws;
    } catch (IOException | MetadataNotFoundException me) {
      return new WriteStatus(false, config.getWriteStatusFailureFraction());
    }
  }

  protected List<Pair<String, HoodieBaseFile>> generateListOfPartitionFiles(HoodieEngineContext context,
                                                                            String baseLocation, List<String> partitions) {
    context.setJobStatus(SparkInsertOverwriteCommitActionExecutor.class.getSimpleName(), String.format(
        "Generating partition to files map for partitions %s ", partitions));
    return context.flatMap(partitions, partition -> {
      Configuration hadoopConf = table.getHadoopConf();
      Path partitionPath = new Path(baseLocation, partition);
      FileSystem fileSystem = partitionPath.getFileSystem(hadoopConf);
      List<Pair<String, HoodieBaseFile>> filteredFiles = Arrays.stream(fileSystem.listStatus(partitionPath))
          .filter(fileStatus -> fileStatus.getPath().getName().endsWith(table.getBaseFileExtension()))
          .map(status -> Pair.of(partition, new HoodieBaseFile(status)))
          .collect(toList());
      return filteredFiles.stream();
    }, Math.max(partitions.size(), 1)).stream().collect(toList());
  }
}
