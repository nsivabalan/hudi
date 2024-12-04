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

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.model.HoodieRecordIndexInfo;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.HoodieMergedLogRecordScanner;
import org.apache.hudi.common.table.log.block.HoodieCommandBlock;
import org.apache.hudi.common.table.log.block.HoodieCorruptBlock;
import org.apache.hudi.common.table.log.block.HoodieDataBlock;
import org.apache.hudi.common.table.log.block.HoodieDeleteBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.TableNotFoundException;
import org.apache.hudi.hadoop.fs.CachingPath;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.io.storage.HoodieFileReaderFactory;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.utilities.streamer.HoodieStreamer;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.schema.MessageType;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;
import static org.apache.hudi.common.util.StringUtils.nonEmpty;
import static org.apache.hudi.utilities.UtilHelpers.buildProperties;
import static org.apache.hudi.utilities.UtilHelpers.readConfig;
import static org.apache.hudi.utilities.streamer.HoodieStreamer.Config.DEFAULT_DFS_SOURCE_PROPERTIES;

public class PrintRecordsTool implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(PrintRecordsTool.class);
  private transient JavaSparkContext jsc;
  private Config cfg;
  private TypedProperties props;

  public PrintRecordsTool(JavaSparkContext jsc, Config cfg) {
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
    @Parameter(names = {"--base-path", "-bp"}, description = "Base path for the table", required = false)
    public String basePath = null;

    @Parameter(names = {"--partition-path", "-pp"}, description = "Partition path", required = false)
    public String partitionPath = null;

    @Parameter(names = {"--record-key", "-rk"}, description = "Record key of interest", required = false)
    public String recordKey = null;

    @Parameter(names = {"--file-id", "fd"})
    public String fileId = null;

    @Parameter(names = {"--base-instant-time", "bi"})
    public String baseInstantTime = null;

    @Parameter(names = {"--cols-to-print", "cp"})
    public String colsToPrint = null;

    @Parameter(names = {"--print-all-records", "ar"})
    public Boolean printAllRecords = false;

    @Parameter(names = {"--print-log-blocks-info", "lbi"})
    public Boolean printLogBlocksInfo = false;

    @Parameter(names = {"--log-files-location", "lfl"})
    public String logFiles = null;

    @Parameter(names = {"--log-files-to-trim", "lflt"})
    public Integer logFilesToTrim = -1;

    @Parameter(names = {"--compare-records", "cr"})
    public Boolean compareRecords = false;

    @Parameter(names = {"--new-base-parquet-file", "bpf"})
    public String newBaseParquetFile = null;

    @Parameter(names = {"--props"}, description = "path to properties file on localfs or dfs, with configurations for "
        + "hoodie client, schema provider, key generator and data source. For hoodie client props, sane defaults are "
        + "used, but recommend use to provide basic things like metrics endpoints, hive configs etc. For sources, refer"
        + "to individual classes, for supported properties."
        + " Properties in this file can be overridden by \"--hoodie-conf\"")
    public String propsFilePath = DEFAULT_DFS_SOURCE_PROPERTIES;

    @Parameter(names = {"--spark-master", "-ms"}, description = "Spark master", required = false)
    public String sparkMaster = "local[2]";

    @Parameter(names = {"--spark-memory", "-sm"}, description = "spark memory to use", required = false)
    public String sparkMemory = "4g";

    public List<String> configs = new ArrayList<>();

    @Parameter(names = {"--help", "-h"}, help = true)
    public Boolean help = false;

    @Override
    public String toString() {
      return "ColumnStats {\n"
          + "   --partitionPath " + partitionPath + ", \n"
          + "   --recordKey " + recordKey + ", \n"
          + "   --baseInstantTime " + baseInstantTime + ", \n"
          + "   --fileId " + fileId + ", \n"
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
      return partitionPath.equals(config.partitionPath)
          && recordKey.equals(config.recordKey)
          && fileId.equals(config.fileId)
          && baseInstantTime.equals(config.baseInstantTime)
          && Objects.equals(sparkMaster, config.sparkMaster)
          && Objects.equals(sparkMemory, config.sparkMemory)
          && Objects.equals(propsFilePath, config.propsFilePath)
          && Objects.equals(configs, config.configs);
    }

    @Override
    public int hashCode() {
      return Objects.hash(partitionPath, recordKey, sparkMaster, sparkMemory, propsFilePath, configs, help);
    }

    public static TypedProperties getProps(FileSystem fs, HoodieStreamer.Config cfg) {
      return cfg.propsFilePath.isEmpty()
          ? buildProperties(cfg.configs)
          : readConfig(fs.getConf(), new Path(cfg.propsFilePath), cfg.configs).getProps();
    }
  }

  public static void main(String[] args) {
    final Config cfg = new Config();
    JCommander cmd = new JCommander(cfg, null, args);

    if (cfg.help || args.length == 0) {
      cmd.usage();
      System.exit(1);
    }

    SparkConf sparkConf = UtilHelpers.buildSparkConf("ExecuteSparkSqlJob", cfg.sparkMaster);
    sparkConf.set("spark.executor.memory", cfg.sparkMemory);
    JavaSparkContext jsc = new JavaSparkContext(sparkConf);

    try {
      PrintRecordsTool printRecordsTool = new PrintRecordsTool(jsc, cfg);
      printRecordsTool.run();
    } catch (TableNotFoundException e) {
      LOG.warn(String.format("The table not found not found: [%s].", cfg.basePath), e);
    } catch (Throwable throwable) {
      LOG.error("Failed to print log records " + cfg, throwable);
    } finally {
      jsc.stop();
    }
  }

  public void run() {
    LOG.info(cfg.toString());
    LOG.info(" ****** Printing Log Records ******");
    try {
      HoodieSparkEngineContext context = new HoodieSparkEngineContext(jsc);
      HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setConf(jsc.hadoopConfiguration()).setBasePath(cfg.basePath)
          .setLoadActiveTimelineOnLoad(true).build();
      TableSchemaResolver schemaResolver = new TableSchemaResolver(metaClient);
      HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder().withPath(cfg.basePath)
          .withSchema(schemaResolver.getTableAvroSchema().toString()).withProperties(props).build();
      printRecs(writeConfig, context, metaClient, context.hadoopConfiguration());
    } catch (Exception e) {
      throw new HoodieException("failed to print records");
    }
  }

  @VisibleForTesting
  void printRecs(HoodieWriteConfig writeConfig, HoodieSparkEngineContext context, HoodieTableMetaClient metaClient, Configuration configuration) throws Exception {
    HoodieTable table = HoodieSparkTable.create(writeConfig, context, metaClient);
    printLogRecords(table, cfg.recordKey == null ? Collections.emptySet()
        : Arrays.stream(cfg.recordKey.split(",")).map(key -> key.trim()).collect(Collectors.toSet()), configuration);
  }

  private void printLogRecords(HoodieTable hoodieTable, Set<String> keysToFilter, Configuration configuration) throws Exception {
    FileSystem fs = hoodieTable.getMetaClient().getFs();
    if (cfg.logFiles == null) {
      Pair<String, String> partitionPathFileIDPair = Pair.of(cfg.partitionPath, cfg.fileId);
      LOG.info("Looking for record key " + Arrays.toString(keysToFilter.toArray()) + " in partition " + partitionPathFileIDPair.getKey()
          + ", fileID " + partitionPathFileIDPair.getValue() + ", with base instant time " + cfg.baseInstantTime);
      Option<FileSlice> fileSliceOption = getMatchingFileSlice(cfg.baseInstantTime, partitionPathFileIDPair, hoodieTable);
      if (fileSliceOption.isPresent()) {
        List<Path> logFilePaths = fileSliceOption.get().getLogFiles().sorted(HoodieLogFile.getLogFileComparator())
            .map(l -> l.getPath()).collect(toList());
        printRecordsFromLogFiles(logFilePaths, fs, keysToFilter, null);
      }
    } else if (!cfg.compareRecords) {
      LOG.info("Looking for record key " + Arrays.toString(keysToFilter.toArray()) + " in log files " + cfg.logFiles);
      List<Path> logFilePaths = Arrays.stream(cfg.logFiles.split(",")).map(key -> key.trim()).map(file -> new Path(file)).collect(toList());
      printRecordsFromLogFiles(logFilePaths, fs, keysToFilter, null);

      LOG.info("\n\n\nAlso printing final snapshot from all log files (size :" + logFilePaths.size() +")");

      readRecordsForGroupWithLogs(cfg.basePath, logFilePaths.stream().map(path -> path.toString()).collect(toList()),
          "20240915203755867", cfg.partitionPath, keysToFilter);
    } else {
      LOG.info("Comparing log records base file records");
      LOG.info("Looking for record key " + Arrays.toString(keysToFilter.toArray()) + " in log files " + cfg.logFiles);
      List<Path> logFilePaths = Arrays.stream(cfg.logFiles.split(",")).map(key -> key.trim()).map(file -> new Path(file)).collect(toList());
      Map<String, GenericRecord> logRecords = new HashMap<>();
      printRecordsFromLogFiles(logFilePaths, fs, keysToFilter, logRecords);

      Map<String, GenericRecord> baseFileRecords = new HashMap<>();
      printRecordsFromBasefile(cfg.newBaseParquetFile, keysToFilter, baseFileRecords, hoodieTable.getHadoopConf());

      AtomicInteger totalMatched = new AtomicInteger(0);
      keysToFilter.forEach(entry -> {
        if (logRecords.containsKey(entry) && baseFileRecords.containsKey(entry)) {
          if (compareTwoRecords(logRecords.get(entry), baseFileRecords.get(entry))) {
            LOG.info("Both log record and base file record matched for " + entry);
            totalMatched.getAndIncrement();
          }
        }
      });

      LOG.info("Total matched records after comparison :: " + totalMatched.get());

      LOG.info("\n\n\nAlso printing final snapshot from all log files");

      readRecordsForGroupWithLogs(cfg.basePath, logFilePaths.stream().map(path -> path.toString()).collect(toList()),
          "20240917083023513", cfg.partitionPath, keysToFilter);
    }
  }

  /**
   * Read records from baseFiles, apply updates and convert to RDD.
   */
  private void readRecordsForGroupWithLogs(String basePath, List<String> logFilePaths, String latestInstant,
                                           String partitionPath, Set<String> keystoFilter) throws Exception {
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setBasePath(basePath).setConf(jsc.hadoopConfiguration()).build();
    TableSchemaResolver schemaResolver = new TableSchemaResolver(metaClient);
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(cfg.basePath)
        .withSchema(schemaResolver.getTableAvroSchema().toString()).withProperties(props).build();

    Schema readerSchema = HoodieAvroUtils.addMetadataFields(new Schema.Parser().parse(config.getSchema()));
    HoodieMergedLogRecordScanner scanner = HoodieMergedLogRecordScanner.newBuilder()
        .withFileSystem(metaClient.getFs())
        .withBasePath(metaClient.getBasePath())
        .withLogFilePaths(logFilePaths)
        .withReaderSchema(readerSchema)
        .withLatestInstantTime(latestInstant)
        .withMaxMemorySizeInBytes(1024*1024*1024L)
        .withReadBlocksLazily(config.getCompactionLazyBlockReadEnabled())
        .withReverseReader(config.getCompactionReverseLogReadEnabled())
        .withBufferSize(config.getMaxDFSStreamBufferSize())
        .withSpillableMapBasePath(config.getSpillableMapBasePath())
        .withPartition(partitionPath)
        .withOptimizedLogBlocksScan(config.enableOptimizedLogBlocksScan())
        .withDiskMapType(config.getCommonConfig().getSpillableDiskMapType())
        .withBitCaskDiskMapCompressionEnabled(config.getCommonConfig().isBitCaskDiskMapCompressionEnabled())
        .withRecordMerger(config.getRecordMerger())
        .withTableMetaClient(metaClient)
        .build();

    for (Map.Entry<String, HoodieRecord> entry : scanner.getRecords().entrySet()) {
      if (keystoFilter != null && !keystoFilter.isEmpty()) {
        if (keystoFilter.contains(entry.getKey())) {
          LOG.info("======= Matching record found after reading snapshot from all log files ");
        }
      }
    }
  }


  private boolean compareTwoRecords(GenericRecord genericRecord1, GenericRecord genericRecord2) {
    List<Schema.Field> fields = genericRecord1.getSchema().getFields();
    boolean misMatch = true;
    for (Schema.Field field : fields) {
      if (!field.name().startsWith("_hoodie")) {
        if ((genericRecord1.get(field.name()) == null && genericRecord2.get(field.name()) != null)
            || (genericRecord1.get(field.name()) != null && genericRecord2.get(field.name()) == null)) {
          misMatch = false;
        } else if (genericRecord1.get(field.name()) != null && genericRecord2.get(field.name()) != null
            && !genericRecord1.get(field.name()).equals(genericRecord2.get(field.name()))) {
          misMatch = false;
        }
      }
      if (misMatch) {
        break;
      }
    }
    return true;
  }

  private void printRecordsFromBasefile(String baseFile, Set<String> keysToFilter, Map<String, GenericRecord> baseFileRecords,
                                        Configuration conf) throws IOException {
    LOG.info("Looking up base file " + baseFile + " for records");
    HoodieFileReader baseFileReader = HoodieFileReaderFactory.getReaderFactory(HoodieRecord.HoodieRecordType.AVRO)
        .getFileReader(new HoodieConfig(), conf, new Path(baseFile));
    ClosableIterator<HoodieRecord> iter = baseFileReader.getRecordIterator();
    while (iter.hasNext()) {
      HoodieRecord rec = iter.next();
      if (keysToFilter.contains((((GenericRecord) rec.getData()).get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString()))) {
        LOG.info("Found matching record in base file for " + (((GenericRecord) rec.getData()).get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString()));
        baseFileRecords.put((((GenericRecord) rec.getData()).get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString()), (GenericRecord) rec.getData());
      }
    }
  }

  private void printRecordsFromLogFiles(List<Path> logFilePaths, FileSystem fs, Set<String> keysToFilter, Map<String, GenericRecord> logRecords) throws IOException {
    LOG.info("Log files for the matching file slice " + Arrays.toString(logFilePaths.toArray()));
    AtomicInteger totalMatchedRecords = new AtomicInteger(0);
    int maxLogFiles = cfg.logFilesToTrim != -1 ? 1000 : cfg.logFilesToTrim;
    int logFilesProcessed = 0;
    for (Path logFile : logFilePaths) {
      if (logFilesProcessed++ >= maxLogFiles) {
        break;
      }
      LOG.info("Processing log file " + logFile.getName());
      MessageType schema = TableSchemaResolver.readSchemaFromLogFile(fs, new CachingPath(logFile.toString()));
      Schema writerSchema = schema != null
          ? new AvroSchemaConverter().convert(Objects.requireNonNull(schema)) : null;
      HoodieLogFormat.Reader reader =
          HoodieLogFormat.newReader(fs, new HoodieLogFile(new CachingPath(logFile.toString())), writerSchema);
      // read the avro blocks
      while (reader.hasNext()) {
        HoodieLogBlock n = reader.next();
        String fileName = n.getBlockContentLocation().get().getLogFile().getFileName();
        if (n instanceof HoodieDataBlock) {
          LOG.info("Processing next block " + fileName + ", log block type " + n.getBlockType()
              + ", instant time " + n.getLogBlockHeader().get(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME) + ", in file " + logFile.getName());
          HoodieDataBlock blk = (HoodieDataBlock) n;
          HoodieLogBlock.HoodieLogBlockType logBlockType = blk.getBlockType();
          try (ClosableIterator<HoodieRecord<IndexedRecord>> recordItr = blk.getRecordIterator(HoodieRecord.HoodieRecordType.AVRO)) {
            int counter = 0;
            while (recordItr.hasNext()) {
              HoodieRecord<IndexedRecord> next = recordItr.next();
              counter++;
              printHoodieRecord(next, keysToFilter, fileName, logBlockType, totalMatchedRecords, logRecords,
                  n.getLogBlockHeader().get(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME));
            }
            if (cfg.printLogBlocksInfo) {
              // if print only log blocks info,
              LOG.info("Processed " + counter + " records from " + fileName);
            }
          }
          LOG.info("Finished processing " + fileName);
        } else if (n instanceof HoodieDeleteBlock) {
          LOG.info("Encountered delete block " + n.getLogBlockHeader().get(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME)
              + ", in file " + logFile.getName());
          LOG.info("Total records in delete command block " + ((HoodieDeleteBlock) n).getRecordsToDelete().length);
          DeleteRecord[] deleteRecords = ((HoodieDeleteBlock) n).getRecordsToDelete();
          Arrays.stream(deleteRecords).forEach(deleteRecord -> {
            if (keysToFilter.contains(deleteRecord.getRecordKey())) {
              LOG.info("============= Matching Record :: Record key deleted in this instant time " + n.getLogBlockHeader().get(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME)
                  + " block, rk : " + deleteRecord.getRecordKey() + ", ordering value " + deleteRecord.getOrderingValue()
                  + ", partition path " + deleteRecord.getPartitionPath() + ", file name " + logFile.getName());
            }
          });
        } else if (n instanceof HoodieCorruptBlock) {
          LOG.info("Encountered corrupt block at " + fileName);
        } else if (n instanceof HoodieCommandBlock) {
          LOG.info("Encountered command block at " + fileName + ", and block type " + ((HoodieCommandBlock) n).getType().name()
              + ", with target instant : " + n.getLogBlockHeader().get(HoodieLogBlock.HeaderMetadataType.TARGET_INSTANT_TIME));
        }
      }
      LOG.info("Closing reader for " + reader.getLogFile().getFileName());
      reader.close();
    }
    LOG.info("Total matched records " + totalMatchedRecords.get());
  }

  private void printHoodieRecord(HoodieRecord<IndexedRecord> next, Set<String> keysToFilter, String fileName,
                                 HoodieLogBlock.HoodieLogBlockType logBlockType, AtomicInteger totalMatchedRecords,
                                 Map<String, GenericRecord> logRecords, String logBlockInstantTime) {
    if (cfg.printAllRecords) {
      //LOG.info("Record " + ((GenericRecord) next.getData()).get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString()
        //  + " " + ((GenericRecord) next.getData()).toString());
      if (fileName.contains("record-index-")) {
        String keyInValue = (String) ((GenericRecord) next.getData()).get("key");
        if (keyInValue.equals("624c9866b6324dab563f8912")) {
          //LOG.info("Record Matched : " + ((GenericRecord) next.getData()).toString());
          GenericRecord genericRecord = (GenericRecord) ((GenericRecord) next.getData()).get("recordIndexMetadata");
          LOG.info("Record matched, high bits : " + genericRecord.get("fileIdHighBits") + ", low bits " + genericRecord.get("fileIdLowBits") + ", instant time "
              + genericRecord.get("instantTime") + ", fileId " +
              HoodieTableMetadataUtil.getLocationFromRecordIndexInfo("", 0, (Long) genericRecord.get("fileIdHighBits"), (Long) genericRecord.get("fileIdLowBits"),
                  1, "", (Long) genericRecord.get("instantTime")));
        }
      } else {
        HashMap<UTF8String, GenericRecord> fsMap = (HashMap<UTF8String, GenericRecord>) ((GenericRecord) next.getData()).get("filesystemMetadata");
        LOG.info("Record matched :: ");
        fsMap.entrySet().forEach((kv) -> {
          LOG.info("      " + kv.getKey() + " -> " + kv.getValue().toString());
        });
      }

      totalMatchedRecords.incrementAndGet();
    } else if (!cfg.printLogBlocksInfo) {
      if (logBlockType == HoodieLogBlock.HoodieLogBlockType.AVRO_DATA_BLOCK || logBlockType == HoodieLogBlock.HoodieLogBlockType.PARQUET_DATA_BLOCK
          || logBlockType == HoodieLogBlock.HoodieLogBlockType.HFILE_DATA_BLOCK) {
        // print matching records
        if (keysToFilter.contains(((GenericRecord) next.getData()).get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString())) {
          LOG.info("============= Matching Record " + ((GenericRecord) next.getData()).get(HoodieRecord.RECORD_KEY_METADATA_FIELD)
              + " found in block w/ instant time : " + logBlockInstantTime + ", and file name : " + fileName + " ============== ");
          totalMatchedRecords.incrementAndGet();
          if (logRecords != null) {
            logRecords.put((((GenericRecord) next.getData()).get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString()), (GenericRecord) next.getData());
          }
          if (cfg.colsToPrint != null) {
            Arrays.stream(cfg.colsToPrint.split(",")).forEach(colToPrint -> {
              LOG.info("Record value for " + colToPrint + " -> " + ((GenericRecord) next.getData()).get(colToPrint));
            });
          } else {
            LOG.info("Record data " + ((GenericRecord) next.getData()).toString());
          }
        }
      }
    }
  }

  private Option<FileSlice> getMatchingFileSlice(String instantTime, Pair<String, String> partitionPathFileIDPair, HoodieTable hoodieTable) {
    AtomicReference<Option<FileSlice>> sliceToReturn = new AtomicReference<>(Option.empty());
    if (nonEmpty(instantTime)
        && hoodieTable.getMetaClient().getCommitsTimeline().filterCompletedInstants().lastInstant().isPresent()) {
      hoodieTable
          .getHoodieView()
          .getAllFileGroups(partitionPathFileIDPair.getKey())
          .filter(hoodieFileGroup -> hoodieFileGroup.getFileGroupId().getFileId().equals(partitionPathFileIDPair.getValue()))
          .forEach(hoodieFileGroup -> {
            if (hoodieFileGroup.getAllFileSlices().filter(fileSlice -> fileSlice.getBaseInstantTime().equals(instantTime)).findFirst().isPresent()) {
              sliceToReturn.set(Option.of(hoodieFileGroup.getAllFileSlices().filter(fileSlice -> fileSlice.getBaseInstantTime().equals(instantTime)).findFirst().get()));
            }
          });
    }
    return sliceToReturn.get();
  }
}
