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
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.WorkloadProfile;
import org.apache.hudi.table.WorkloadStat;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SparkRestorePartitionsCommitActionExecutor<T>
    extends SparkInsertOverwriteCommitActionExecutor<T> {

  private static final Logger LOG = LoggerFactory.getLogger(SparkRestorePartitionsCommitActionExecutor.class);

  private static final String RESTORE_SUFFIX = "0";
  private static final String SUCCESSFULLY_RESTORED = "restore.partition.successful";
  private static final String FAILED_TO_RESTORE_ALREADY_EXISTS = "restore.partition.failed.already.present";
  private static final String FAILED_TO_RESTORE_NOT_FOUND = "restore.partition.failed.missing.at.stashed";
  private static final String STASHED_LOCATION_KEY = "StashedLocation";

  private String srcStashLocation;
  private List<String> partitions;

  public SparkRestorePartitionsCommitActionExecutor(HoodieEngineContext context,
                                                   HoodieWriteConfig config, HoodieTable table,
                                                   String instantTime, List<String> partitions,
                                                   String absStashLocation) {
    super(context, config, table, instantTime, null, WriteOperationType.DELETE_PARTITION, Option.of(new HashMap<>()));
    this.partitions = partitions;
    this.srcStashLocation = absStashLocation;
  }

  private Map<String, List<String>> initOpStatus() {
    Map<String, List<String>> status = new HashMap<>();
    status.put(SUCCESSFULLY_RESTORED, new ArrayList<>());
    status.put(FAILED_TO_RESTORE_ALREADY_EXISTS, new ArrayList<>());
    status.put(FAILED_TO_RESTORE_NOT_FOUND, new ArrayList<>());
    return status;
  }

  private List<String> validatePartitionsTobeRestored(List<String> partitions, Map<String, List<String>> opStatus) {
    List<String> tobeRestored = new ArrayList<>(partitions);
    partitions.forEach(partitionPath -> {
      try {
        Path srcStashLocationPath = new Path(srcStashLocation);
        Path srcPath = new Path(srcStashLocation, partitionPath);
        Path dstPath = new Path(table.getMetaClient().getBasePath(), partitionPath);
        FileSystem srcFs = srcStashLocationPath.getFileSystem(table.getHadoopConf());
        FileSystem dstFs = table.getMetaClient().getFs();
        if (!srcFs.exists(srcPath)) {
          opStatus.get(FAILED_TO_RESTORE_NOT_FOUND).add(partitionPath);
          LOG.warn(String.format("Partition %s not found at Stashed location %s."
              + " Restore operation failed", partitionPath, srcStashLocation));
          tobeRestored.remove(partitionPath);
        }
        if (dstFs.exists(dstPath)) {
          boolean alreadyExists = true;
          if (dstFs.isDirectory(dstPath) && dstFs.listStatus(dstPath).length == 0) {
            alreadyExists = !dstFs.delete(dstPath, true);
          }
          if (alreadyExists) {
            opStatus.get(FAILED_TO_RESTORE_ALREADY_EXISTS).add(partitionPath);
            LOG.warn(String.format("Partition %s already exists. Restore operation failed", dstPath));
            tobeRestored.remove(partitionPath);
          }
        }
      } catch (IOException e) {
        LOG.warn(String.format("Restore operation failed failed for partition %s", partitionPath));
        opStatus.get(FAILED_TO_RESTORE_NOT_FOUND).add(partitionPath);
        tobeRestored.remove(partitionPath);
      }
    });
    return tobeRestored;
  }

  @Override
  public HoodieWriteMetadata<HoodieData<WriteStatus>> execute() {
    Map<String, List<String>> opStatus = initOpStatus();
    JavaSparkContext jsc = HoodieSparkEngineContext.getSparkContext(context);
    HoodieTimer timer = HoodieTimer.start();

    this.saveWorkloadProfileMetadataToInflight(new WorkloadProfile(Pair.of(new HashMap<>(), new WorkloadStat())), instantTime);

    partitions = validatePartitionsTobeRestored(partitions, opStatus);

    List<Pair<String, HoodieBaseFile>> partitionFiles = generateListOfPartitionFiles(context, srcStashLocation, partitions);

    LOG.info(String.format("Found %d files in %d partitions", partitionFiles.size(), partitions.size()));
    LOG.info("PartitionFiles : " + partitionFiles.toString());
    partitions.forEach(partitionPath -> {
      FileSystem dstFs = table.getMetaClient().getFs();
      Path srcPath = new Path(srcStashLocation, partitionPath);
      Path dstPath = new Path(table.getMetaClient().getBasePath(), partitionPath);
      try {
        if (!table.getMetaClient().getFs().exists(dstPath.getParent())) {
          table.getMetaClient().getFs().mkdirs(dstPath.getParent());
        }
        if (!dstFs.exists(dstPath)) {
          LOG.info(String.format("Restoring %s -> %s", srcPath, dstPath));
          dstFs.rename(srcPath, dstPath);
          opStatus.get(SUCCESSFULLY_RESTORED).add(partitionPath);
        } else {
          LOG.warn("Failed to restore partition as " + dstPath + " already exists");
          opStatus.get(FAILED_TO_RESTORE_ALREADY_EXISTS).add(partitionPath);
        }
      } catch (IOException e) {
        LOG.warn("Failed to restore partition with exception" + partitionPath);
        opStatus.get(FAILED_TO_RESTORE_NOT_FOUND).add(partitionPath);
      }
    });

    HoodieData<WriteStatus> writeStatusesJavaRDD = HoodieJavaRDD.of(getWriteStatusForRestoredFiles(partitionFiles));

    HoodieWriteMetadata<HoodieData<WriteStatus>> result = new HoodieWriteMetadata<>();
    this.extraMetadata.get().putIfAbsent(STASHED_LOCATION_KEY, srcStashLocation);
    this.extraMetadata.get().putIfAbsent(FAILED_TO_RESTORE_ALREADY_EXISTS,
        opStatus.get(FAILED_TO_RESTORE_ALREADY_EXISTS).toString());
    this.extraMetadata.get().putIfAbsent(FAILED_TO_RESTORE_NOT_FOUND,
        opStatus.get(FAILED_TO_RESTORE_NOT_FOUND).toString());
    this.extraMetadata.get().putIfAbsent(SUCCESSFULLY_RESTORED,
        opStatus.get(SUCCESSFULLY_RESTORED).toString());
    result.setWriteStatuses(writeStatusesJavaRDD);
    result.setWriteStats(writeStatusesJavaRDD.map(WriteStatus::getStat).collectAsList());
    result.setIndexUpdateDuration(Duration.ofMillis(timer.endTimer()));

    this.commitOnAutoCommit(result);
    return result;
  }

  private JavaRDD<WriteStatus> getWriteStatusForRestoredFiles(List<Pair<String, HoodieBaseFile>> partitionBaseFiles) {
    context.setJobStatus(this.getClass().getName(), String.format("Generating writeStatus for files restored from %s", srcStashLocation));
    JavaSparkContext jsc = HoodieSparkEngineContext.getSparkContext(context);
    if (partitionBaseFiles.isEmpty()) {
      return jsc.emptyRDD();
    }
    return jsc.parallelize(partitionBaseFiles, partitionBaseFiles.size()).map(pbf -> {
      String baseFileName = pbf.getValue().getFileName();
      Path srcPartitionPath = new Path(srcStashLocation, pbf.getKey());
      Path dstPartitionPath = new Path(table.getMetaClient().getBasePath(), pbf.getKey());
      String restoredFileId = FSUtils.getFileId(baseFileName) + RESTORE_SUFFIX;
      String writeToken = FSUtils.getWriteToken(baseFileName);
      String commitTS = FSUtils.getCommitTime(baseFileName);
      Path originalFilePath = new Path(dstPartitionPath, baseFileName);
      Path restoredFilePath = new Path(dstPartitionPath, FSUtils.makeBaseFileName(commitTS, writeToken, restoredFileId));
      try {
        LOG.info("Renaming  file " + originalFilePath + " -> " + restoredFilePath);
        table.getMetaClient().getFs().rename(originalFilePath, restoredFilePath);

        return generateWriteStatus(pbf.getValue(), restoredFileId, pbf.getKey(), table.getMetaClient().getBasePath(),
            restoredFilePath);
      } catch (IOException e) {
        LOG.error("Failed to rename " + originalFilePath + " -> " + restoredFilePath);
        throw e;
      }
    });
  }
}
