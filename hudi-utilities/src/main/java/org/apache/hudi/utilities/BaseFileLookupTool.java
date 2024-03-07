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

import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.function.SerializablePairFunction;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.TableNotFoundException;
import org.apache.hudi.index.bloom.BloomIndexFileInfo;
import org.apache.hudi.index.bloom.SparkHoodieBloomIndexHelper;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.io.storage.HoodieFileReaderFactory;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.utilities.streamer.HoodieStreamer;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
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
import java.util.stream.Collectors;

import static org.apache.hudi.utilities.UtilHelpers.buildProperties;
import static org.apache.hudi.utilities.UtilHelpers.readConfig;
import static org.apache.hudi.utilities.streamer.HoodieStreamer.Config.DEFAULT_DFS_SOURCE_PROPERTIES;

public class BaseFileLookupTool implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(PrintRecordsTool.class);
  private transient JavaSparkContext jsc;
  private Config cfg;
  private TypedProperties props;

  public BaseFileLookupTool(JavaSparkContext jsc, Config cfg) {
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

    @Parameter(names = {"--base-file-location", "-bf"}, description = "Base file location", required = false)
    public String baseFileLocation = null;

    @Parameter(names = {"--partition-path", "-pp"}, description = "Partition path", required = false)
    public String partitionPath = null;

    @Parameter(names = {"--base-file-name", "-bfn"}, description = "Base file name", required = false)
    public String baseFileName = null;

    @Parameter(names = {"--record-keys", "-rk"}, description = "Record key of interest", required = false)
    public String recordKeys = null;

    @Parameter(names = {"--min-max-pruning", "mmp"})
    public Boolean minMaxPruning = false;

    @Parameter(names = {"--bloom-filter-look-up", "bfl"})
    public Boolean bloomFilterLookup = false;

    @Parameter(names = {"--lookup-in-file", "lf"})
    public Boolean lookupInFile = null;

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
          + "   --basePath " + basePath + ", \n"
          + "   --baseFileLocation " + baseFileLocation + ", \n"
          + "   --minMaxPruning " + minMaxPruning + ", \n"
          + "   --bloomFilterLookup " + bloomFilterLookup + ", \n"
          + "   --lookupInFile " + lookupInFile + ", \n"
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
          && baseFileLocation.equals(config.baseFileLocation)
          && minMaxPruning.equals(config.minMaxPruning)
          && bloomFilterLookup.equals(config.bloomFilterLookup)
          && Objects.equals(sparkMaster, config.sparkMaster)
          && Objects.equals(sparkMemory, config.sparkMemory)
          && Objects.equals(propsFilePath, config.propsFilePath)
          && Objects.equals(configs, config.configs);
    }

    @Override
    public int hashCode() {
      return Objects.hash(basePath, basePath, minMaxPruning, bloomFilterLookup, sparkMaster, sparkMemory, propsFilePath, configs, help);
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
      BaseFileLookupTool baseFileLookupTool = new BaseFileLookupTool(jsc, cfg);
      baseFileLookupTool.run();
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
      lookupInBaseFile(writeConfig, context, metaClient);
    } catch (Exception e) {
      throw new HoodieException("failed to print records");
    }
  }

  @VisibleForTesting
  void lookupInBaseFile(HoodieWriteConfig writeConfig, HoodieSparkEngineContext context, HoodieTableMetaClient metaClient) throws IOException {
    HoodieTable table = HoodieSparkTable.create(writeConfig, context, metaClient);
    lookupInBaseFile(writeConfig, table, context, cfg.recordKeys == null ? Collections.emptySet() :
        Arrays.stream(cfg.recordKeys.split(",")).map(key -> key.trim()).collect(Collectors.toSet()));
  }

  private void lookupInBaseFile(HoodieWriteConfig writeConfig, HoodieTable hoodieTable, HoodieSparkEngineContext context,
                                Set<String> keysToFilter) throws IOException {
    FileSystem fs = hoodieTable.getMetaClient().getFs();
    Path baseFilePath = new Path(cfg.baseFileLocation);
    HoodieBaseFile hoodieBaseFile = new HoodieBaseFile(fs.getFileStatus(baseFilePath));
    HoodieFileReader fileReader = createNewFileReader(hoodieBaseFile, writeConfig, hoodieTable.getHadoopConf());
    String[] minMaxValues = fileReader.readMinMaxRecordKeys();
    LOG.info("Min Max values " + minMaxValues[0] + " -> " + minMaxValues[1]);
    BloomIndexFileInfo bloomIndexFileInfo = new BloomIndexFileInfo(hoodieBaseFile.getFileId(), minMaxValues[0], minMaxValues[1]);
    AtomicInteger matchedEntries = new AtomicInteger();
    keysToFilter.forEach(rk -> {
      LOG.info("Looking up record key " + rk + " based on min max values " + bloomIndexFileInfo.isKeyInRange(rk));
      if (bloomIndexFileInfo.isKeyInRange(rk)) {
        matchedEntries.incrementAndGet();
      }
    });
    LOG.info("Total keys looked up " + keysToFilter.size() + ", matched entries " + matchedEntries.get());

    LOG.info("Bloom filter lookup");

    SparkHoodieBloomIndexHelper bloomIndexHelper = SparkHoodieBloomIndexHelper.getInstance();
    List<Pair<String, String>> partitionFilePairs = Collections.singletonList(Pair.of(cfg.partitionPath, cfg.baseFileName));
    HoodiePairData<String, String> partitionFileIdPair = context.parallelize(partitionFilePairs, 1)
        .mapToPair((SerializablePairFunction<Pair<String, String>, String, String>) t -> t);
    String fileId = FSUtils.getFileId(cfg.baseFileName);
    HoodieFileGroupId fileGroupId = new HoodieFileGroupId(cfg.partitionPath, fileId);
    HoodiePairData<HoodieFileGroupId, String> fileComparisonsPair = context
        .parallelize(new ArrayList<>(keysToFilter))
        .mapToPair((SerializablePairFunction<String, HoodieFileGroupId, String>) t -> Pair.of(fileGroupId, t));

    Map<String, List<BloomIndexFileInfo>> partitionToFileInfo = new HashMap<>();
    partitionToFileInfo.put(cfg.partitionPath, Collections.singletonList(bloomIndexFileInfo));

    List<Pair<HoodieKey, HoodieRecordLocation>> keyToRecLocation = bloomIndexHelper.findMatchingFilesForRecordKeys(writeConfig, context, hoodieTable, partitionFileIdPair,
        fileComparisonsPair, partitionToFileInfo, Collections.emptyMap()).collectAsList();
    keyToRecLocation.forEach(entry -> {
      LOG.info("Rec key " + entry.getKey() + ", location " + entry.getValue().toString());
    });

    fileReader.close();
  }

  protected HoodieFileReader createNewFileReader(HoodieBaseFile hoodieBaseFile, HoodieWriteConfig config, Configuration conf) throws IOException {
    return HoodieFileReaderFactory.getReaderFactory(config.getRecordMerger().getRecordType())
        .getFileReader(config, conf, new Path(hoodieBaseFile.getPath()));
  }
}
