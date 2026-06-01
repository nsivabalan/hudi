/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.utilities.streamer;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.UUID;

import static org.apache.hudi.common.table.HoodieTableMetaClient.SAMPLE_WRITES_FOLDER_PATH;
import static org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator.getInstantFromTemporalAccessor;
import static org.apache.hudi.config.HoodieCompactionConfig.COPY_ON_WRITE_RECORD_SIZE_ESTIMATE;
import static org.apache.hudi.utilities.config.HoodieStreamerConfig.SAMPLE_WRITES_ENABLED;
import static org.apache.hudi.utilities.config.HoodieStreamerConfig.SAMPLE_WRITES_SIZE;

/**
 * The utilities class is dedicated to estimating average record size by writing sample incoming records
 * to `.hoodie/.aux/.sample_writes/<instant time>/<epoch millis>` and reading the commit metadata.
 * <p>
 * TODO handle sample_writes sub-path clean-up w.r.t. rollback and insert overwrite. (HUDI-6044)
 */
public class SparkSampleWritesUtils {

  private static final Logger LOG = LoggerFactory.getLogger(SparkSampleWritesUtils.class);

  public static Option<HoodieWriteConfig> getWriteConfigWithRecordSizeEstimate(JavaSparkContext jsc, Option<JavaRDD<HoodieRecord>> recordsOpt, HoodieWriteConfig writeConfig) {
    return new SparkSampleWritesUtils().getWriteConfigWithRecordSizeEstimateInternal(jsc, recordsOpt, writeConfig);
  }

  protected Option<HoodieWriteConfig> getWriteConfigWithRecordSizeEstimateInternal(JavaSparkContext jsc, Option<JavaRDD<HoodieRecord>> recordsOpt, HoodieWriteConfig writeConfig) {
    if (!writeConfig.getBoolean(SAMPLE_WRITES_ENABLED)) {
      LOG.debug("Skip overwriting record size estimate as it's disabled.");
      return Option.empty();
    }
    HoodieTableMetaClient metaClient = getMetaClient(jsc, writeConfig.getBasePath());
    if (metaClient.isTimelineNonEmpty()) {
      LOG.info("Skip overwriting record size estimate due to timeline is non-empty.");
      return Option.empty();
    }
    try {
      String instantTime = getInstantFromTemporalAccessor(Instant.now().atZone(ZoneId.systemDefault()));
      Option<Long> result = doSampleWrites(jsc, recordsOpt, writeConfig, instantTime);
      if (result.isPresent()) {
        LOG.info("Overwriting record size estimate to " + result.get());
        TypedProperties props = writeConfig.getProps();
        props.put(COPY_ON_WRITE_RECORD_SIZE_ESTIMATE.key(), String.valueOf(result.get()));
        return Option.of(HoodieWriteConfig.newBuilder().withProperties(props).build());
      }
    } catch (Exception e) {
      LOG.error(String.format("Not overwriting record size estimate for table %s due to error when doing sample writes.", writeConfig.getTableName()), e);
    }
    return Option.empty();
  }

  protected Option<Long> doSampleWrites(JavaSparkContext jsc, Option<JavaRDD<HoodieRecord>> recordsOpt, HoodieWriteConfig writeConfig, String instantTime)
      throws IOException {
    String uniqueId = UUID.randomUUID().toString();
    final String sampleWritesBasePath = getSampleWritesBasePath(jsc, writeConfig, uniqueId);
    // Propagate the user's configured write table version to the sample-writes shadow table so
    // that the on-disk table layout matches the version that the inherited write config (and the
    // SparkRDDWriteClient below) will operate with. Otherwise the shadow table is initialized at
    // HoodieTableVersion.current() while the write config carries hoodie.write.table.version=N,
    // which produces version-mismatched reads/writes on the sample-writes table.
    HoodieTableMetaClient.newTableBuilder()
        .setTableType(HoodieTableType.COPY_ON_WRITE)
        .setTableName(String.format("%s_samples_%s", writeConfig.getTableName(), uniqueId))
        .setTableVersion(writeConfig.getWriteVersion())
        .setCDCEnabled(false)
        .initTable(HadoopFSUtils.getStorageConfWithCopy(jsc.hadoopConfiguration()), sampleWritesBasePath);
    TypedProperties props = writeConfig.getProps();
    props.put(SAMPLE_WRITES_ENABLED.key(), "false");
    props.setProperty(HoodieCompactionConfig.PARQUET_SMALL_FILE_LIMIT.key(), "100"); // during average record size estimation, we only consider file's whose size is > (1.0 * small file size),
    // where 1.0 = record size estimation threshold, and OOB small file size is 100Mb. Hence overriding the small file size to 100 bytes so that every file in commit metadata is accounted for.
    final HoodieWriteConfig sampleWriteConfig = HoodieWriteConfig.newBuilder()
        .withProps(props)
        .withTableServicesEnabled(false)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false).build())
        .withSchemaEvolutionEnable(false)
        .withBulkInsertParallelism(1)
        .withPath(sampleWritesBasePath)
        .withWriteTableVersion(writeConfig.getWriteVersion().versionCode())
        .withRecordSizeEstimatorAverageMetadataSize(writeConfig.getRecordSizeEstimatorAverageMetadataSize())
        .withStorageConfig(HoodieStorageConfig.newBuilder().parquetCompressionCodec(writeConfig.getParquetCompressionCodec()).build())
        .build();
    try (SparkRDDWriteClient sampleWriteClient =  new SparkRDDWriteClient(new HoodieSparkEngineContext(jsc), sampleWriteConfig, Option.empty())) {
      int size = writeConfig.getIntOrDefault(SAMPLE_WRITES_SIZE);
      if (recordsOpt.isPresent()) {
        JavaRDD<HoodieRecord> records = recordsOpt.get();
        List<HoodieRecord> samples = records.coalesce(1).take(size);
        if (samples.isEmpty()) {
          return Option.empty();
        }
        HoodieTableMetaClient sampleWritesMetaClient = getMetaClient(jsc, sampleWritesBasePath);
        sampleWriteClient.startCommit(Option.of(instantTime), sampleWritesMetaClient.getCommitActionType(), sampleWritesMetaClient);
        JavaRDD<WriteStatus> writeStatusRDD = bulkIngestAndGetWriteStatus(sampleWriteClient, jsc, samples, instantTime);
        if (writeStatusRDD.filter(WriteStatus::hasErrors).count() > 0) {
          LOG.error("sample writes for table {} failed with errors.", writeConfig.getTableName());
          if (LOG.isTraceEnabled()) {
            LOG.trace("Printing out the top 100 errors");
            writeStatusRDD.filter(WriteStatus::hasErrors).take(100).forEach(ws -> {
              LOG.trace("Global error :", ws.getGlobalError());
              ws.getErrors().forEach((key, throwable) ->
                  LOG.trace(String.format("Error for key: %s", key), throwable));
            });
          }
          return Option.empty();
        } else {
          return Option.of(computeAvgBytesFromWriteStatuses(writeStatusRDD, writeConfig));
        }
      } else {
        return Option.empty();
      }
    }
  }

  protected JavaRDD<WriteStatus> bulkIngestAndGetWriteStatus(SparkRDDWriteClient sparkRDDWriteClient, JavaSparkContext jsc, List<HoodieRecord> recordsToIngest, String instantTime) {
    return sparkRDDWriteClient.bulkInsert(jsc.parallelize(recordsToIngest, 1), instantTime);
  }

  private static String getSampleWritesBasePath(JavaSparkContext jsc, HoodieWriteConfig writeConfig, String uniqueId) throws IOException {
    StoragePath basePath = new StoragePath(writeConfig.getBasePath(), SAMPLE_WRITES_FOLDER_PATH + StoragePath.SEPARATOR + uniqueId);
    HoodieStorage storage = getMetaClient(jsc, writeConfig.getBasePath()).getStorage();
    if (storage.exists(basePath)) {
      storage.deleteDirectory(basePath);
    }
    return basePath.toString();
  }

  private static long computeAvgBytesFromWriteStatuses(JavaRDD<WriteStatus> writeStatusRDD, HoodieWriteConfig writeConfig) {
    final long metadataSizeEstimate = writeConfig.getRecordSizeEstimatorAverageMetadataSize();
    final long commitSizeThreshold = (long) (writeConfig.getRecordSizeEstimationThreshold() * writeConfig.getParquetSmallFileLimit());
    List<WriteStatus> writeStatuses = writeStatusRDD.collect();
    long totalBytesWritten = writeStatuses.stream().mapToLong(ws -> ws.getStat().getTotalWriteBytes()).sum()
        - writeStatuses.size() * metadataSizeEstimate;
    long totalRecordsWritten = writeStatuses.stream().mapToLong(ws -> ws.getStat().getNumWrites()).sum();
    if (totalBytesWritten > commitSizeThreshold && totalRecordsWritten > 0) {
      return (long) Math.ceil(1.0 * totalBytesWritten / totalRecordsWritten);
    }
    return writeConfig.getCopyOnWriteRecordSizeEstimate();
  }

  private static HoodieTableMetaClient getMetaClient(JavaSparkContext jsc, String basePath) {
    FileSystem fs = HadoopFSUtils.getFs(basePath, jsc.hadoopConfiguration());
    return HoodieTableMetaClient.builder()
        .setConf(HadoopFSUtils.getStorageConfWithCopy(fs.getConf())).setBasePath(basePath).build();
  }
}
