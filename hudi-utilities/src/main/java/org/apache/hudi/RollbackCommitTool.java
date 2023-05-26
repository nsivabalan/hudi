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

import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.function.SerializableFunction;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.TableNotFoundException;
import org.apache.hudi.utilities.IdentitySplitter;
import org.apache.hudi.utilities.TableSizeStats;
import org.apache.hudi.utilities.UtilHelpers;

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
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class RollbackCommitTool implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(RollbackCommitTool.class);

  // Spark context
  private transient JavaSparkContext jsc;
  // config
  private Config cfg;
  // Properties with source, hoodie client, key generator etc.
  private TypedProperties props;

  public RollbackCommitTool(JavaSparkContext jsc, Config cfg) {
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
   * @param cfg {@link TableSizeStats.Config} instance.
   * @return the {@link TypedProperties} instance.
   */
  private TypedProperties readConfigFromFileSystem(JavaSparkContext jsc, Config cfg) {
    return UtilHelpers.readConfig(jsc.hadoopConfiguration(), new Path(cfg.propsFilePath), cfg.configs)
        .getProps(true);
  }

  public static class Config implements Serializable {
    @Parameter(names = {"--base-path", "-bp"}, description = "Base path for the table", required = false)
    public String basePath = null;

    @Parameter(names = {"--commit-time-to-rollback", "-rb"}, description = "Commit time to rollback.", required = false)
    public String commitTimeToRollback = null;

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
      return "RollbackCommitTool {\n"
          + "   --base-path " + basePath + ", \n"
          + "   --commit-time-to-rollback " + commitTimeToRollback + ", \n"
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
          && Objects.equals(commitTimeToRollback, config.commitTimeToRollback)
          && Objects.equals(parallelism, config.parallelism)
          && Objects.equals(sparkMaster, config.sparkMaster)
          && Objects.equals(sparkMemory, config.sparkMemory)
          && Objects.equals(propsFilePath, config.propsFilePath)
          && Objects.equals(configs, config.configs);
    }

    @Override
    public int hashCode() {
      return Objects.hash(basePath, commitTimeToRollback, parallelism, sparkMaster, sparkMemory, propsFilePath, configs, help);
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
      RollbackCommitTool rollbackCommitTool = new RollbackCommitTool(jsc, cfg);
      rollbackCommitTool.run();
    } catch (TableNotFoundException e) {
      LOG.warn(String.format("The Hudi data table is not found: [%s].", cfg.basePath), e);
    } catch (Throwable throwable) {
      LOG.error("Failed to get table size stats for " + cfg, throwable);
    } finally {
      jsc.stop();
    }
  }

  public void run() {
    try {
      LOG.info(cfg.toString());
      LOG.info(" ****** Rolling back commit ******");
      if (cfg.basePath == null) {
        throw new HoodieIOException("Base path needs to be set.");
      }
      rollbackCommit(cfg.commitTimeToRollback);
    } catch (Exception e) {
      throw new HoodieException("Unable to do fetch table size stats." + cfg.basePath, e);
    }
  }

  private void rollbackCommit(String commitTime) throws IOException {
    rollbackCommit(cfg.basePath, jsc.hadoopConfiguration(), commitTime);
    deleteCommitMetaFiles(cfg.basePath, commitTime, "commit");
    Path metadataPath = new Path(cfg.basePath + "/.hoodie/metadata");
    FileSystem fs = metadataPath.getFileSystem(jsc.hadoopConfiguration());
    LOG.info("Checking if metadata needs to be deleted " + fs.exists(metadataPath));
    if (fs.exists(metadataPath)) {
      rollbackCommit(metadataPath.toString(), jsc.hadoopConfiguration(), commitTime);
      deleteCommitMetaFiles(metadataPath.toString(), commitTime, "deltacommit");
    }
  }

  private void deleteCommitMetaFiles(String basePath, String commitTime, String action) throws IOException {
    FileSystem fs = new Path(basePath).getFileSystem(jsc.hadoopConfiguration());
    LOG.info("Deleting commit meta file " + commitTime);
    fs.delete(new Path(basePath + "/.hoodie/" + commitTime + "." + action));
    LOG.info("Deleting commit meta file " + commitTime + ".inflight");
    if (action.equals("commit")) {
      fs.delete(new Path(basePath + "/.hoodie/" + commitTime + ".inflight"));
    } else {
      fs.delete(new Path(basePath + "/.hoodie/" + commitTime + "." + action + ".inflight"));
    }
    LOG.info("Deleting commit meta file " + commitTime + ".requested");
    fs.delete(new Path(basePath + "/.hoodie/" + commitTime + "." + action + ".requested"));
  }

  private void rollbackCommit(String basePath, Configuration configuration, String commitTime) throws IOException {
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setBasePath(basePath).setConf(configuration).build();
    HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();
    HoodieInstant instant = activeTimeline.getCommitsTimeline().filterCompletedInstants().filter(entry ->
        entry.getTimestamp().equals(commitTime)).firstInstant().get();

    HoodieCommitMetadata commitMetadata =
        HoodieCommitMetadata.fromBytes(activeTimeline.getInstantDetails(instant).get(), HoodieCommitMetadata.class);

    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);
    SerializableConfiguration serializableConfiguration = new SerializableConfiguration(configuration);

    List<String> paths = commitMetadata.getWriteStats().stream().map(writeStat -> writeStat.getPath()).collect(Collectors.toList());
    LOG.info("List of files being deleted " + paths.size());
    paths.forEach(entry -> LOG.info("   File to delete " + entry));
    List<Boolean> result = engineContext.parallelize(paths, paths.size()).map((SerializableFunction<String, Boolean>) v1 -> {
      FileSystem fs = new Path(v1).getFileSystem(serializableConfiguration.get());
      fs.delete(new Path(basePath + "/" + v1));
      return true;
    }).collectAsList();

  }
}
