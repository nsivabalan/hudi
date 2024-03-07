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

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieColumnRangeMetadata;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.ParquetUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.TableNotFoundException;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class ColumnStatsSparkJob implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(ColumnStatsSparkJob.class);

  // Spark context
  private transient JavaSparkContext jsc;
  // config
  private Config cfg;
  // Properties with source, hoodie client, key generator etc.
  private TypedProperties props;

  public ColumnStatsSparkJob(JavaSparkContext jsc, Config cfg) {
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

    @Parameter(names = {"--partition-path", "-ppath"}, description = "Partition of interest", required = false)
    public String partitionPath = null;

    @Parameter(names = {"--base-file", "-bf"}, description = "Base file of interest", required = false)
    public String baseFile = null;

    @Parameter(names = {"--columns", "-c"}, description = "Comma separated list of columns to fetch stats.", required = false)
    public String columns = null;

    @Parameter(names = {"--props-path", "-pp"}, description = "Properties file containing base paths one per line", required = false)
    public String propsFilePath = null;

    @Parameter(names = {"--parallelism", "-pl"}, description = "Parallelism for valuation", required = false)
    public int parallelism = 200;

    @Parameter(names = {"--spark-master", "-ms"}, description = "Spark master", required = false)
    public String sparkMaster = null;

    @Parameter(names = {"--spark-memory", "-sm"}, description = "spark memory to use", required = false)
    public String sparkMemory = "1g";

    public List<String> configs = new ArrayList<>();

    @Parameter(names = {"--help", "-h"}, help = true)
    public Boolean help = false;

    @Override
    public String toString() {
      return "ColumnStats {\n"
          + "   --base-path " + basePath + ", \n"
          + "   --partition-path " + partitionPath + ", \n"
          + "   --columns " + columns + ", \n"
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
          && Objects.equals(partitionPath, config.partitionPath)
          && Objects.equals(columns, config.columns)
          && Objects.equals(parallelism, config.parallelism)
          && Objects.equals(sparkMaster, config.sparkMaster)
          && Objects.equals(sparkMemory, config.sparkMemory)
          && Objects.equals(propsFilePath, config.propsFilePath)
          && Objects.equals(configs, config.configs);
    }

    @Override
    public int hashCode() {
      return Objects.hash(basePath, partitionPath, columns, parallelism, sparkMaster, sparkMemory, propsFilePath, configs, help);
    }
  }

  public static void main(String[] args) {
    final Config cfg = new Config();
    JCommander cmd = new JCommander(cfg, null, args);

    if (cfg.help || args.length == 0) {
      cmd.usage();
      System.exit(1);
    }

    SparkConf sparkConf = UtilHelpers.buildSparkConf("Table-Size-Stats", cfg.sparkMaster);
    sparkConf.set("spark.executor.memory", cfg.sparkMemory);
    JavaSparkContext jsc = new JavaSparkContext(sparkConf);

    try {
      ColumnStatsSparkJob columnStatsSparkJob = new ColumnStatsSparkJob(jsc, cfg);
      columnStatsSparkJob.run();
    } catch (TableNotFoundException e) {
      LOG.warn(String.format("The Hudi data table is not found: [%s].", cfg.basePath), e);
    } catch (Throwable throwable) {
      LOG.error("Failed to get col stats for " + cfg, throwable);
    } finally {
      jsc.stop();
    }
  }

  public void run() {
    try {
      LOG.info(cfg.toString());
      LOG.info(" ****** Fetching column stats ******");
      if (cfg.basePath == null) {
        throw new HoodieIOException("Base path needs to be set.");
      }
      logColumnsStats(cfg.basePath, cfg.partitionPath, cfg.baseFile, cfg.columns);

    } catch (Exception e) {
      throw new HoodieException("Unable to do fetch columns stats." + cfg.basePath, e);
    }
  }

  private void logColumnsStats(String basePath, String partitionPath, String baseFileToInspect, String columns) throws IOException {

    LOG.warn("Processing table " + basePath);
    SerializableConfiguration serializableConfiguration = new SerializableConfiguration(jsc.hadoopConfiguration());

    HoodieTableMetaClient metaClientLocal = HoodieTableMetaClient.builder()
        .setBasePath(basePath)
        .setConf(serializableConfiguration.get()).build();
    HoodieMetadataConfig metadataConfig1 = HoodieMetadataConfig.newBuilder()
        .enable(false)
        .build();
    HoodieTableFileSystemView fileSystemView = FileSystemViewManager
        .createInMemoryFileSystemView(new HoodieLocalEngineContext(serializableConfiguration.get()),
            metaClientLocal, metadataConfig1);
    List<String> columnsToIndex = Collections.singletonList(columns);

    if (baseFileToInspect == null) {
      List<HoodieBaseFile> baseFiles = fileSystemView.getLatestBaseFiles(partitionPath).collect(Collectors.toList());
      baseFiles.forEach(baseFile -> {
        LOG.info("Processing file " + baseFile.getFileName());
        String pathStr = baseFile.getHadoopPath().toString();
        String filePartitionPath = pathStr.startsWith("/") ? pathStr.substring(1) : pathStr;
        HoodieColumnRangeMetadata<Comparable> columnRangeMetadata =
            readColumnRangeMetadataFrom(filePartitionPath, metaClientLocal, columnsToIndex).get(0);
        LOG.info("Column " + columns + " stats : " + columnRangeMetadata.getValueCount()
            + ", min value " + columnRangeMetadata.getMinValue() + ", max value " + columnRangeMetadata.getMaxValue());
      });
    } else {
      LOG.info("Inspecting base file " + baseFileToInspect);
      HoodieColumnRangeMetadata<Comparable> columnRangeMetadata =
          readColumnRangeMetadataFrom(baseFileToInspect, metaClientLocal, columnsToIndex).get(0);
      LOG.info("Column " + columns + " stats : " + columnRangeMetadata.getValueCount()
          + ", min value " + columnRangeMetadata.getMinValue() + ", max value " + columnRangeMetadata.getMaxValue());
    }

    LOG.info("Completed fetching col stats for table : {}, partition {} and column {}", basePath, partitionPath, columns);
  }

  private static List<HoodieColumnRangeMetadata<Comparable>> readColumnRangeMetadataFrom(String filePath,
                                                                                         HoodieTableMetaClient datasetMetaClient,
                                                                                         List<String> columnsToIndex) {
    try {
      if (filePath.endsWith(HoodieFileFormat.PARQUET.getFileExtension())) {
        Path fullFilePath = new Path(datasetMetaClient.getBasePath(), filePath);
        LOG.info("File path to inspect " + fullFilePath);
        List<HoodieColumnRangeMetadata<Comparable>> columnRangeMetadataList =
            new ParquetUtils().readRangeFromParquetMetadata(datasetMetaClient.getHadoopConf(), fullFilePath, columnsToIndex);

        return columnRangeMetadataList;
      }

      LOG.warn("Column range index not supported for: " + filePath);
      return Collections.emptyList();
    } catch (Exception e) {
      // NOTE: In case reading column range metadata from individual file failed,
      //       we simply fall back, in lieu of failing the whole task
      LOG.error("Failed to fetch column range metadata for: " + filePath);
      return Collections.emptyList();
    }
  }
}