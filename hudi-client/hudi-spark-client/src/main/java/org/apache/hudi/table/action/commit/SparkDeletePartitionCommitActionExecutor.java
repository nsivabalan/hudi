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

import org.apache.hudi.avro.model.HoodieRequestedReplaceMetadata;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.client.utils.DeletePartitionUtils;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaPairRDD;
import org.apache.hudi.exception.HoodieDeletePartitionException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.WorkloadProfile;
import org.apache.hudi.table.WorkloadStat;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.common.commitmetadata.DeletePartitionCommitMetadata.FAILED_TO_DELETE;
import static org.apache.hudi.common.commitmetadata.DeletePartitionCommitMetadata.STASHED_LOCATION_KEY;
import static org.apache.hudi.common.commitmetadata.DeletePartitionCommitMetadata.SUCCESSFULLY_DELETED;
import static org.apache.hudi.common.table.timeline.HoodieInstant.State.REQUESTED;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.REPLACE_COMMIT_ACTION;

public class SparkDeletePartitionCommitActionExecutor<T>
    extends SparkInsertOverwriteCommitActionExecutor<T> {

  private static final Logger LOG = LoggerFactory.getLogger(SparkDeletePartitionCommitActionExecutor.class);

  private List<String> partitions;
  private Option<String> dstBackupLocation;

  public SparkDeletePartitionCommitActionExecutor(HoodieEngineContext context,
                                                  HoodieWriteConfig config, HoodieTable table,
                                                  String instantTime, List<String> partitions,
                                                  Option<String> absBackupLocation) {
    super(context, config, table, instantTime, null, WriteOperationType.DELETE_PARTITION, Option.of(new HashMap<>()));
    this.partitions = partitions;
    this.dstBackupLocation = absBackupLocation;
  }

  private Map<String, List<String>> initOpStatus() {
    Map<String, List<String>> status = new HashMap<>();
    status.put(SUCCESSFULLY_DELETED, new ArrayList<>());
    status.put(FAILED_TO_DELETE, new ArrayList<>());
    return status;
  }

  private List<String> validatePartitionsTobeDeleted(List<String> partitions, Map<String, List<String>> opStatus) {
    List<String> tobeDeleted = new ArrayList<>(partitions);
    if (!dstBackupLocation.isPresent() || dstBackupLocation.get().isEmpty()) {
      return tobeDeleted;
    }
    partitions.forEach(partitionPath -> {
      try {
        Path srcPath = new Path(table.getMetaClient().getBasePath(), partitionPath);
        FileSystem srcFs = table.getMetaClient().getFs();
        Path dstPath = new Path(dstBackupLocation.get(), partitionPath);
        FileSystem dstFs = dstPath.getFileSystem(context.getHadoopConf().get());
        if (!srcFs.exists(srcPath)) {
          opStatus.get(FAILED_TO_DELETE).add(partitionPath);
          LOG.warn(String.format("Partition %s not found at basepath location %s."
              + " Delete operation failed", partitionPath, table.getMetaClient().getBasePath()));
          tobeDeleted.remove(partitionPath);
        }
        if (dstFs.exists(dstPath)) {
          if (dstFs.isDirectory(dstPath) && dstFs.listStatus(dstPath).length == 0) {
            dstFs.delete(dstPath, true);
          } else {
            opStatus.get(FAILED_TO_DELETE).add(partitionPath);
            LOG.warn(String.format("Partition %s already exists at %s. Delete operation failed", partitionPath, dstPath));
            tobeDeleted.remove(partitionPath);
          }
        }
      } catch (IOException e) {
        LOG.warn(String.format("Delete operation failed failed for partition %s", partitionPath));
        opStatus.get(FAILED_TO_DELETE).add(partitionPath);
        tobeDeleted.remove(partitionPath);
      }
    });
    return tobeDeleted;
  }

  @Override
  public HoodieWriteMetadata<HoodieData<WriteStatus>> execute() {
    try {
      DeletePartitionUtils.checkForPendingTableServiceActions(table, partitions);
      Map<String, List<String>> opStatus = initOpStatus();
      JavaSparkContext jsc = HoodieSparkEngineContext.getSparkContext(context);
      context.setJobStatus(this.getClass().getSimpleName(), "Gather all file ids from all deleting partitions.");
      HoodieTimer timer = HoodieTimer.start();
      partitions = validatePartitionsTobeDeleted(partitions, opStatus);
      Map<String, List<String>> partitionToReplaceFileIds = jsc.parallelize(partitions, Math.max(1, partitions.size())).distinct()
          .mapToPair(partitionPath -> new Tuple2<>(partitionPath, getAllExistingFileIds(partitionPath))).collectAsMap();
      if (dstBackupLocation.isPresent() && !dstBackupLocation.get().isEmpty()) {
        performDeletePartitionsWithBackup(opStatus, partitionToReplaceFileIds);
      } else {
        opStatus.get(SUCCESSFULLY_DELETED).addAll(partitions);
        this.extraMetadata.get().putIfAbsent(SUCCESSFULLY_DELETED, opStatus.get(SUCCESSFULLY_DELETED).toString());
      }
      createRequestedInflightFiles();
      HoodieWriteMetadata<HoodieData<WriteStatus>> result = new HoodieWriteMetadata<>();
      result.setPartitionToReplaceFileIds(partitionToReplaceFileIds);
      result.setIndexUpdateDuration(Duration.ofMillis(timer.endTimer()));
      result.setWriteStatuses(context.emptyHoodieData());
      this.commitOnAutoCommit(result);
      return result;
    } catch (Exception e) {
      throw new HoodieDeletePartitionException("Failed to drop partitions for commit time " + instantTime, e);
    }
  }

  private void performDeletePartitionsWithBackup(Map<String, List<String>> opStatus,
                                                 Map<String, List<String>> partitionToReplaceFileIds) {
    List<Pair<String, HoodieBaseFile>> partitionFiles = generateListOfPartitionFiles(context,
        table.getMetaClient().getBasePath(), partitions);

    List<String> tobeDeleted = new ArrayList<>(partitions);
    LOG.info("Partitions to Be deleted : " + tobeDeleted);
    partitions.forEach(partitionPath -> {
      try {
        Path srcPath = new Path(table.getMetaClient().getBasePath(), partitionPath);
        FileSystem srcFs = table.getMetaClient().getFs();
        Path dstPath = new Path(dstBackupLocation.get(), partitionPath);
        FileSystem dstFs = dstPath.getFileSystem(context.getHadoopConf().get());
        if (!dstFs.exists(dstPath.getParent())) {
          dstFs.mkdirs(dstPath.getParent());
        }
        if (!dstFs.exists(dstPath)) {
          dstFs.rename(srcPath, dstPath);
          LOG.info(String.format("Renamed %s -> %s", srcPath, dstPath));
          tobeDeleted.remove(partitionPath);
          opStatus.get(SUCCESSFULLY_DELETED).add(partitionPath);
          dstFs.mkdirs(srcPath);
        }
      } catch (IOException e) {
        LOG.warn("Failed to delete partition " + partitionPath);
        tobeDeleted.remove(partitionPath);
        partitionToReplaceFileIds.remove(partitionPath);
        opStatus.get(FAILED_TO_DELETE).add(partitionPath);
      }
    });

    LOG.info("Committing metadata for instant " + instantTime);
    this.extraMetadata.get().putIfAbsent(STASHED_LOCATION_KEY, dstBackupLocation.get());
    this.extraMetadata.get().putIfAbsent(SUCCESSFULLY_DELETED, opStatus.get(SUCCESSFULLY_DELETED).toString());
    this.extraMetadata.get().putIfAbsent(FAILED_TO_DELETE, opStatus.get(FAILED_TO_DELETE).toString());
  }

  private void createRequestedInflightFiles() {
    try {
      HoodieInstant dropPartitionsInstant = new HoodieInstant(REQUESTED, REPLACE_COMMIT_ACTION, instantTime);
      if (!table.getMetaClient().getFs().exists(new Path(table.getMetaClient().getMetaPath(),
          dropPartitionsInstant.getFileName()))) {
        HoodieRequestedReplaceMetadata requestedReplaceMetadata = HoodieRequestedReplaceMetadata.newBuilder()
            .setOperationType(WriteOperationType.DELETE_PARTITION.name())
            .setExtraMetadata(extraMetadata.orElse(Collections.emptyMap()))
            .build();
        table.getMetaClient().getActiveTimeline().saveToPendingReplaceCommit(dropPartitionsInstant,
            TimelineMetadataUtils.serializeRequestedReplaceMetadata(requestedReplaceMetadata));
      }
      this.saveWorkloadProfileMetadataToInflight(new WorkloadProfile(Pair.of(new HashMap<>(), new WorkloadStat())),
          instantTime);
    } catch (IOException ioe) {
      throw new HoodieIOException("Failed to create Requested/Inflight files for " + instantTime, ioe);
    }
  }
}
