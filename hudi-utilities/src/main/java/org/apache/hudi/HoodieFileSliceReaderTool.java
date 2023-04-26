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

package org.apache.hudi;

import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.HoodieMergedLogRecordScanner;
import org.apache.hudi.common.table.log.block.HoodieDataBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieMemoryConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.TableNotFoundException;
import org.apache.hudi.utilities.IdentitySplitter;
import org.apache.hudi.utilities.UtilHelpers;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class HoodieFileSliceReaderTool implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieFileSliceReaderTool.class);

  // Spark context
  private transient JavaSparkContext jsc;
  // config
  private Config cfg;
  // Properties with source, hoodie client, key generator etc.
  private TypedProperties props;

  public HoodieFileSliceReaderTool(JavaSparkContext jsc, Config cfg) {
    this.jsc = jsc;
    this.cfg = cfg;

    this.props = cfg.propsFilePath == null
        ? UtilHelpers.buildProperties(cfg.configs)
        : readConfigFromFileSystem(jsc, cfg);
  }

  /**
   * Reads config from the file system.
   *
   * @param jsc {@link JavaSparkContext} instance.
   * @param cfg {@link Config} instance.
   * @return the {@link TypedProperties} instance.
   */
  private TypedProperties readConfigFromFileSystem(JavaSparkContext jsc, Config cfg) {
    return UtilHelpers.readConfig(jsc.hadoopConfiguration(), new Path(cfg.propsFilePath), cfg.configs)
        .getProps(true);
  }

  public static class Config implements Serializable {
    @Parameter(names = {"--base-path", "-sp"}, description = "Base path for the table", required = false)
    public String basePath = null;

    @Parameter(names = {"--should-merge", "-smerge"}, description = "Merge all records", required = false)
    public Boolean shouldMerge = false;

    @Parameter(names = {"--partition-path", "-ppath"}, description = "partition path of interest", required = false)
    public String partitionPath = "";

    @Parameter(names = {"--non-partitioned", "-np"}, description = "If non partittioned", required = false)
    public Boolean nonPartitioned = false;

    @Parameter(names = {"--fileId", "-fid"}, description = "fileID of interest", required = false)
    public String fileId = "";

    @Parameter(names = {"--baseInstantTime", "-bit"}, description = "base instant time of interest", required = false)
    public String baseInstantTime = "";

    @Parameter(names = {"--filter-for-records", "-ffr"}, description = "Filter for specific record keys", required = false)
    public Boolean filterForRecords = false;

    @Parameter(names = {"--recordKeyFilter", "-rk"}, description = "record key filter", required = false)
    public String recordKey = "";

    @Parameter(names = {"--props-path", "-pp"}, description = "Properties file containing base paths one per line", required = false)
    public String propsFilePath = null;

    @Parameter(names = {"--parallelism", "-pl"}, description = "Parallelism for valuation", required = false)
    public int parallelism = 200;

    @Parameter(names = {"--spark-master", "-ms"}, description = "Spark master", required = false)
    public String sparkMaster = null;

    @Parameter(names = {"--spark-memory", "-sm"}, description = "spark memory to use", required = false)
    public String sparkMemory = "1g";

    @Parameter(names = {"--hoodie-conf"}, description = "Any configuration that can be set in the properties file "
        + "(using the CLI parameter \"--props\") can also be passed command line using this parameter. This can be repeated",
        splitter = IdentitySplitter.class)
    public List<String> configs = new ArrayList<>();

    @Parameter(names = {"--help", "-h"}, help = true)
    public Boolean help = false;

    @Override
    public String toString() {
      return "HoodieFileSliceReaderConfig {\n"
          + "   --base-path " + basePath + ", \n"
          + "   --parallelism " + parallelism + ", \n"
          + "   --spark-master " + sparkMaster + ", \n"
          + "   --spark-memory " + sparkMemory + ", \n"
          + "   --props " + propsFilePath + ", \n"
          + "   --hoodie-conf " + configs
          + "\n}";
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Config config = (Config) o;
      return basePath.equals(config.basePath)
          && Objects.equals(parallelism, config.parallelism)
          && Objects.equals(sparkMaster, config.sparkMaster)
          && Objects.equals(sparkMemory, config.sparkMemory)
          && Objects.equals(propsFilePath, config.propsFilePath)
          && Objects.equals(configs, config.configs);
    }

    @Override
    public int hashCode() {
      return Objects.hash(basePath, parallelism, sparkMaster, sparkMemory, propsFilePath, configs, help);
    }
  }

  public static void main(String[] args) {
    final Config cfg = new Config();
    JCommander cmd = new JCommander(cfg, null, args);

    if (cfg.help || args.length == 0) {
      cmd.usage();
      System.exit(1);
    }

    SparkConf sparkConf = UtilHelpers.buildSparkConf("Hoodie file slice reader", cfg.sparkMaster);
    sparkConf.set("spark.executor.memory", cfg.sparkMemory);
    JavaSparkContext jsc = new JavaSparkContext(sparkConf);

    try {
      HoodieFileSliceReaderTool validator = new HoodieFileSliceReaderTool(jsc, cfg);
      validator.run();
    } catch (TableNotFoundException e) {
      LOG.warn(String.format("The Hudi data table is not found: [%s]. "
          + "Skipping the validation of the metadata table.", cfg.basePath), e);
    } catch (Throwable throwable) {
      LOG.error("Fail to do hoodie metadata table validation for " + cfg, throwable);
    } finally {
      jsc.stop();
    }
  }

  public void run() {
    try {
      LOG.info(cfg.toString());
      LOG.info(" ****** Fetching records from file slice ******");
      String basePartitionPath = cfg.nonPartitioned ? cfg.basePath : cfg.basePath + "/" + cfg.partitionPath;
      printRecordsFromFileSlice(cfg.basePath, cfg.shouldMerge, basePartitionPath, cfg.fileId, cfg.baseInstantTime, cfg.filterForRecords, cfg.recordKey);
    } catch (Exception e) {
      throw new HoodieException("Unable to do fetch records from file slice " + cfg.basePath, e);
    }
  }

  private void printRecordsFromFileSlice(String basePath, boolean shouldMerge,
                                         String basePartitionPath, String fileId, String baseInstantTime, boolean filterForRecords, String recordKey) throws IOException {
    LOG.warn("Processing file slice " + basePartitionPath + " with fileId " + fileId + ", base instant time "
        + baseInstantTime + ", filter for records " + filterForRecords + ", recordKey " + recordKey);
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
        .setConf(jsc.hadoopConfiguration()).setBasePath(cfg.basePath)
        .setLoadActiveTimelineOnLoad(true)
        .build();

    FileSystem fs = metaClient.getFs();
    FileStatus[] fileStatuses = fs.listStatus(new Path(basePartitionPath));

    List<String> logFilePaths0 = Arrays.stream(fs.listStatus(new Path(basePartitionPath)))
        .filter(fileStatus -> fileStatus.getPath().getName().contains(fileId))
        .map(status -> status.getPath().toString()).sorted(Comparator.reverseOrder())
        .collect(Collectors.toList());

    List<String> logFilePaths1 = Arrays.stream(fs.listStatus(new Path(basePartitionPath)))
        .filter(fileStatus -> fileStatus.getPath().getName().contains(baseInstantTime))
        .map(status -> status.getPath().toString()).sorted(Comparator.reverseOrder())
        .collect(Collectors.toList());

    List<String> logFilePaths = Arrays.stream(fs.listStatus(new Path(basePartitionPath)))
        .filter(fileStatus -> fileStatus.getPath().getName().contains(fileId) && fileStatus.getPath().getName().contains(baseInstantTime)
            && !fileStatus.getPath().getName().contains("parquet"))
        .map(status -> status.getPath().toString()).sorted(Comparator.reverseOrder())
        .collect(Collectors.toList());
    LOG.info("Matched log files " + Arrays.toString(logFilePaths.toArray()));

    if (logFilePaths.isEmpty()) {
      LOG.info("There are no log files to process");
      return;
    }

    // TODO : readerSchema can change across blocks/log files, fix this inside Scanner
    AvroSchemaConverter converter = new AvroSchemaConverter();
    // get schema from last log file
    Schema readerSchema =
        converter.convert(Objects.requireNonNull(TableSchemaResolver.readSchemaFromLogFile(fs, new Path(logFilePaths.get(logFilePaths.size() - 1)))));

    List<IndexedRecord> allRecords = new ArrayList<>();
    if (shouldMerge) {
      LOG.info("===========================> MERGING RECORDS <===================");
      HoodieMergedLogRecordScanner scanner =
          HoodieMergedLogRecordScanner.newBuilder()
              .withFileSystem(fs)
              .withBasePath(metaClient.getBasePath())
              .withLogFilePaths(logFilePaths)
              .withReaderSchema(readerSchema)
              .withLatestInstantTime(
                  metaClient.getActiveTimeline()
                      .getDeltaCommitTimeline().lastInstant().get().getTimestamp())
              .withReadBlocksLazily(
                  Boolean.parseBoolean(
                      HoodieCompactionConfig.COMPACTION_LAZY_BLOCK_READ_ENABLE.defaultValue()))
              .withReverseReader(
                  Boolean.parseBoolean(
                      HoodieCompactionConfig.COMPACTION_REVERSE_LOG_READ_ENABLE.defaultValue()))
              .withBufferSize(HoodieMemoryConfig.MAX_DFS_STREAM_BUFFER_SIZE.defaultValue())
              .withMaxMemorySizeInBytes(
                  HoodieMemoryConfig.DEFAULT_MAX_MEMORY_FOR_SPILLABLE_MAP_IN_BYTES)
              .withSpillableMapBasePath(HoodieMemoryConfig.SPILLABLE_MAP_BASE_PATH.defaultValue())
              .withDiskMapType(HoodieCommonConfig.SPILLABLE_DISK_MAP_TYPE.defaultValue())
              .withBitCaskDiskMapCompressionEnabled(HoodieCommonConfig.DISK_MAP_BITCASK_COMPRESSION_ENABLED.defaultValue())
              .build();
      for (HoodieRecord<? extends HoodieRecordPayload> hoodieRecord : scanner) {
        Option<IndexedRecord> record = hoodieRecord.getData().getInsertValue(readerSchema);
        if (!filterForRecords) {
          allRecords.add(record.get());
        } else {
          GenericRecord genRec = (GenericRecord) record.get();
          if (genRec.get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString().contains(recordKey)) {
            allRecords.add(record.get());
          }
        }
      }
    } else {
      LOG.info("===========================> NOT MERGING RECORDS <===================");
      for (String logFile : logFilePaths) {
        Schema writerSchema = new AvroSchemaConverter()
            .convert(Objects.requireNonNull(TableSchemaResolver.readSchemaFromLogFile(metaClient.getFs(), new Path(logFile))));
        HoodieLogFormat.Reader reader =
            HoodieLogFormat.newReader(fs, new HoodieLogFile(new Path(logFile)), writerSchema);
        // read the avro blocks
        while (reader.hasNext()) {
          HoodieLogBlock n = reader.next();
          if (n instanceof HoodieDataBlock) {
            HoodieDataBlock blk = (HoodieDataBlock) n;
            List<IndexedRecord> records = blk.getRecords();
            for (IndexedRecord record : records) {
              if (!filterForRecords) {
                allRecords.add(record);
              } else {
                GenericRecord genRec = (GenericRecord) record;
                if (genRec.get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString().contains(recordKey)) {
                  allRecords.add(record);
                }
              }
            }
          }
        }
        reader.close();
      }
    }

    LOG.info("Printing out records " + allRecords.size());
    allRecords.forEach(record -> LOG.info("Record " + ((GenericRecord) record).toString()));
  }
}
