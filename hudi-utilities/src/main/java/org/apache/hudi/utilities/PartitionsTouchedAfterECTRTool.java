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

package org.apache.hudi.utilities;

import org.apache.hudi.avro.model.HoodieActionInstant;
import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCleanerPlan;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.CleanerUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.exception.HoodieIOException;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

/**
 * Standalone Java tool that lists partitions that have been <em>replaced</em> or <em>cleaned</em>
 * in the active timeline at or after the last known earliest-commit-to-retain (ECTR). The ECTR
 * is sourced from the most recent completed clean's plan ({@code earliestInstantToRetain}). If
 * no completed clean exists, or that field is null/empty (e.g. the previous clean produced an
 * empty plan), the tool scans the entire active timeline.
 *
 * <p>Considered instants:
 * <ul>
 *   <li>Completed {@code replaceCommit} — both replaced partitions
 *       ({@code partitionToReplaceFileIds}) and partitions written by the same instant
 *       ({@code partitionToWriteStats}).</li>
 *   <li>Completed {@code clean} — partitions present in the clean's
 *       {@code HoodieCleanMetadata.partitionMetadata}.</li>
 * </ul>
 *
 * <p>Plain {@code commit} / {@code deltaCommit} instants are intentionally <strong>not</strong>
 * included: bulk_insert / insert / upsert ingestions that only add new file groups (without
 * any subsequent replaceCommit or clean) are out of scope for this tool.
 *
 * <p>Sample invocation:
 * <pre>
 *   java -cp hudi-utilities-bundle.jar \
 *     org.apache.hudi.utilities.PartitionsTouchedAfterECTRTool \
 *     --base-path s3://bucket/path/to/table \
 *     --output-file /tmp/partitions.txt
 * </pre>
 */
public class PartitionsTouchedAfterECTRTool {

  private static final Logger LOG = LoggerFactory.getLogger(PartitionsTouchedAfterECTRTool.class);

  private final Config cfg;
  private final Configuration hadoopConf;

  public PartitionsTouchedAfterECTRTool(Config cfg, Configuration hadoopConf) {
    this.cfg = cfg;
    this.hadoopConf = hadoopConf;
  }

  public static class Config {
    @Parameter(names = {"--base-path", "-bp"}, description = "Base path of the Hudi table", required = true)
    public String basePath;

    @Parameter(names = {"--output-file", "-o"}, description = "Optional file to write the deduped partition list (one per line)")
    public String outputFile;

    @Parameter(names = {"--help", "-h"}, help = true)
    public boolean help = false;
  }

  public static void main(String[] args) {
    Config cfg = new Config();
    JCommander cmd = JCommander.newBuilder().addObject(cfg).build();
    cmd.parse(args);
    if (cfg.help) {
      cmd.usage();
      System.exit(0);
    }
    try {
      new PartitionsTouchedAfterECTRTool(cfg, new Configuration()).run();
    } catch (Throwable t) {
      LOG.error("Failed to compute partitions touched after ECTR for base path {}", cfg.basePath, t);
      System.exit(1);
    }
  }

  public Set<String> run() throws IOException {
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
        .setConf(hadoopConf)
        .setBasePath(cfg.basePath)
        .setLoadActiveTimelineOnLoad(true)
        .build();

    String lowerBound = resolveLowerBound(metaClient);
    LOG.info("Using lower-bound instant timestamp = {} for base path {}", lowerBound, cfg.basePath);

    HoodieTimeline replaceTimeline = metaClient.getActiveTimeline()
        .getCompletedReplaceTimeline();
    HoodieTimeline cleanTimeline = metaClient.getActiveTimeline()
        .getCleanerTimeline()
        .filterCompletedInstants();

    Set<String> touched = new TreeSet<>();
    long replaceCount = 0;
    for (HoodieInstant instant : replaceTimeline.getInstants()) {
      if (lowerBound != null
          && HoodieTimeline.compareTimestamps(instant.getTimestamp(), HoodieTimeline.LESSER_THAN, lowerBound)) {
        continue;
      }
      Set<String> partitionsForInstant = partitionsReplacedBy(replaceTimeline, instant);
      LOG.info("Replace instant {} touched {} partitions",
          instant.getTimestamp(), partitionsForInstant.size());
      touched.addAll(partitionsForInstant);
      replaceCount++;
    }

    long cleanCount = 0;
    for (HoodieInstant instant : cleanTimeline.getInstants()) {
      if (lowerBound != null
          && HoodieTimeline.compareTimestamps(instant.getTimestamp(), HoodieTimeline.LESSER_THAN, lowerBound)) {
        continue;
      }
      Set<String> partitionsForInstant = partitionsCleanedBy(metaClient, instant);
      LOG.info("Clean instant {} touched {} partitions",
          instant.getTimestamp(), partitionsForInstant.size());
      touched.addAll(partitionsForInstant);
      cleanCount++;
    }

    LOG.info("Scanned {} replaceCommit and {} clean instants at-or-after {}; total distinct partitions touched = {}",
        replaceCount, cleanCount, lowerBound, touched.size());
    for (String partition : touched) {
      LOG.info("  partition: {}", partition);
    }

    if (!StringUtils.isNullOrEmpty(cfg.outputFile)) {
      writeOutput(touched);
    }
    return touched;
  }

  private String resolveLowerBound(HoodieTableMetaClient metaClient) throws IOException {
    Option<HoodieInstant> lastClean = metaClient.getActiveTimeline()
        .getCleanerTimeline()
        .filterCompletedInstants()
        .lastInstant();

    if (!lastClean.isPresent()) {
      LOG.warn("No completed clean instant found on active timeline; reporting all replaced/cleaned partitions across the active timeline");
      return null;
    }

    HoodieInstant cleanInstant = lastClean.get();
    HoodieCleanerPlan plan = CleanerUtils.getCleanerPlan(
        metaClient,
        cleanInstant.isRequested() ? cleanInstant : HoodieTimeline.getCleanRequestedInstant(cleanInstant.getTimestamp()));
    HoodieActionInstant ectr = plan.getEarliestInstantToRetain();
    if (ectr != null && !StringUtils.isNullOrEmpty(ectr.getTimestamp())) {
      LOG.info("Resolved ECTR={} from last completed clean instant {}", ectr.getTimestamp(), cleanInstant.getTimestamp());
      return ectr.getTimestamp();
    }
    LOG.warn("Last completed clean {} has no earliestInstantToRetain in its plan; scanning the entire active timeline",
        cleanInstant.getTimestamp());
    return null;
  }

  private Set<String> partitionsReplacedBy(HoodieTimeline timeline, HoodieInstant instant) {
    try {
      HoodieReplaceCommitMetadata md = HoodieReplaceCommitMetadata.fromBytes(
          timeline.getInstantDetails(instant).get(), HoodieReplaceCommitMetadata.class);
      Set<String> partitions = new HashSet<>();
      partitions.addAll(md.getPartitionToReplaceFileIds().keySet());
      partitions.addAll(md.getPartitionToWriteStats().keySet());
      return partitions;
    } catch (IOException e) {
      throw new HoodieIOException("Failed to read replaceCommit metadata for instant " + instant, e);
    }
  }

  private Set<String> partitionsCleanedBy(HoodieTableMetaClient metaClient, HoodieInstant cleanInstant) {
    try {
      HoodieCleanMetadata md = CleanerUtils.getCleanerMetadata(metaClient, cleanInstant);
      if (md.getPartitionMetadata() == null) {
        return new HashSet<>();
      }
      return new HashSet<>(md.getPartitionMetadata().keySet());
    } catch (IOException e) {
      throw new HoodieIOException("Failed to read clean metadata for instant " + cleanInstant, e);
    }
  }

  private void writeOutput(Set<String> partitions) throws IOException {
    Path outPath = new Path(cfg.outputFile);
    FileSystem fs = outPath.getFileSystem(hadoopConf);
    try (BufferedWriter writer = new BufferedWriter(
        new OutputStreamWriter(fs.create(outPath, true), StandardCharsets.UTF_8))) {
      for (String partition : partitions) {
        writer.write(partition);
        writer.newLine();
      }
    }
    LOG.info("Wrote {} partitions to {}", partitions.size(), cfg.outputFile);
  }
}
