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
import org.apache.hudi.avro.model.HoodieCleanPartitionMetadata;
import org.apache.hudi.avro.model.HoodieCleanerPlan;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.versioning.clean.CleanPlanV2MigrationHandler;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.util.Option;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestPartitionsTouchedAfterECTRTool extends HoodieCommonTestHarness {

  private HoodieTestTable testTable;

  @BeforeEach
  void setUp() throws IOException {
    initMetaClient();
    testTable = HoodieTestTable.of(metaClient);
  }

  @Test
  void noCleanInstant_returnsOnlyReplacedAndCleanedPartitions() throws Exception {
    // Plain commits / deltaCommits are out of scope for this tool.
    testTable.addCommit("001", Option.of(commitMetadataFor("p_ingest_a")));
    testTable.addCommit("002", Option.of(commitMetadataFor("p_ingest_b")));
    addReplaceCommit("003", Collections.singleton("p_replaced_x"), Collections.singleton("p_new_x"));
    addDeletePartitionsCommit("004", Collections.singleton("p_deleted_y"));

    Set<String> partitions = runTool();

    assertEquals(setOf("p_replaced_x", "p_new_x", "p_deleted_y"), partitions);
  }

  @Test
  void cleanWithValidEctr_returnsOnlyReplacedAndCleanedPartitionsAtOrAfterEctr() throws Exception {
    // Pre-ECTR — should be excluded.
    testTable.addCommit("001", Option.of(commitMetadataFor("p_pre_clean_a")));
    addReplaceCommit("002", Collections.singleton("p_pre_clean_b"), Collections.singleton("p_pre_clean_c"));
    // Clean at 005 records ECTR = "010" => everything at/after 010 is in scope.
    // The clean itself is at 005, strictly before the ECTR, so its cleaned partition
    // is also excluded.
    addCleanWithEctr("005", "010", Collections.singleton("p_cleaned_pre"));
    // Post-ECTR — ingestion commit still excluded; replace + delete-partition included.
    testTable.addCommit("011", Option.of(commitMetadataFor("p_post_ingest")));
    addReplaceCommit("012", Collections.singleton("p_replaced_post"), Collections.singleton("p_new_post"));
    addDeletePartitionsCommit("013", Collections.singleton("p_deleted_post"));
    // Another clean at 014 (>= ECTR) that actually deleted files in p_cleaned_post.
    addCleanWithEctr("014", "010", Collections.singleton("p_cleaned_post"));

    Set<String> partitions = runTool();

    assertEquals(
        setOf("p_replaced_post", "p_new_post", "p_deleted_post", "p_cleaned_post"),
        partitions);
  }

  @Test
  void cleanWithEmptyEctr_returnsAllReplacedAndCleanedPartitions() throws Exception {
    testTable.addCommit("001", Option.of(commitMetadataFor("p_ingest")));
    addReplaceCommit("002", Collections.singleton("p_replaced"), Collections.singleton("p_new"));
    addDeletePartitionsCommit("003", Collections.singleton("p_deleted"));
    // Clean exists, but its plan has no earliestInstantToRetain — mirrors the
    // empty-clean case (CleanerUtils.getEarliestCommitToRetain == empty).
    addCleanWithEctr("004", null, Collections.emptySet());

    Set<String> partitions = runTool();

    assertEquals(setOf("p_replaced", "p_new", "p_deleted"), partitions);
  }

  @Test
  void instantsStrictlyBeforeEctr_areExcluded() throws Exception {
    // Pre-ECTR replace + clean — excluded.
    addReplaceCommit("001", Collections.singleton("p_replaced_before"), Collections.emptySet());
    addCleanWithEctr("003", "005", Collections.singleton("p_cleaned_before"));
    // At/after ECTR — included.
    addReplaceCommit("006", Collections.singleton("p_replaced_at_or_after"), Collections.emptySet());
    addCleanWithEctr("007", "005", Collections.singleton("p_cleaned_at_or_after"));

    Set<String> partitions = runTool();

    assertTrue(partitions.contains("p_replaced_at_or_after"));
    assertTrue(partitions.contains("p_cleaned_at_or_after"));
    assertEquals(setOf("p_replaced_at_or_after", "p_cleaned_at_or_after"), partitions);
  }

  @Test
  void ingestionOnlyCommitsAreIgnored() throws Exception {
    // Only plain commits / deltaCommits on the timeline — tool should report nothing.
    testTable.addCommit("001", Option.of(commitMetadataFor("p_ingest_a")));
    testTable.addCommit("002", Option.of(commitMetadataFor("p_ingest_b")));

    Set<String> partitions = runTool();

    assertEquals(Collections.emptySet(), partitions);
  }

  // ----- helpers -----

  private Set<String> runTool() throws IOException {
    PartitionsTouchedAfterECTRTool.Config cfg = new PartitionsTouchedAfterECTRTool.Config();
    cfg.basePath = basePath;
    return new PartitionsTouchedAfterECTRTool(cfg, new Configuration()).run();
  }

  private HoodieCommitMetadata commitMetadataFor(String... partitions) {
    HoodieCommitMetadata md = new HoodieCommitMetadata();
    md.setOperationType(WriteOperationType.UPSERT);
    for (String p : partitions) {
      HoodieWriteStat stat = new HoodieWriteStat();
      stat.setPartitionPath(p);
      stat.setFileId(UUID.randomUUID().toString());
      stat.setPath(p + "/" + UUID.randomUUID() + ".parquet");
      md.addWriteStat(p, stat);
    }
    return md;
  }

  private void addReplaceCommit(String instantTime, Set<String> replacedPartitions, Set<String> newWritePartitions)
      throws Exception {
    HoodieReplaceCommitMetadata md = new HoodieReplaceCommitMetadata();
    md.setOperationType(WriteOperationType.INSERT_OVERWRITE);
    for (String p : replacedPartitions) {
      md.addReplaceFileId(p, UUID.randomUUID().toString());
    }
    for (String p : newWritePartitions) {
      HoodieWriteStat stat = new HoodieWriteStat();
      stat.setPartitionPath(p);
      stat.setFileId(UUID.randomUUID().toString());
      stat.setPath(p + "/" + UUID.randomUUID() + ".parquet");
      md.addWriteStat(p, stat);
    }
    testTable.addReplaceCommit(instantTime, Option.empty(), Option.empty(), md);
  }

  private void addDeletePartitionsCommit(String instantTime, Set<String> deletedPartitions) throws Exception {
    HoodieReplaceCommitMetadata md = new HoodieReplaceCommitMetadata();
    md.setOperationType(WriteOperationType.DELETE_PARTITION);
    for (String p : deletedPartitions) {
      md.addReplaceFileId(p, UUID.randomUUID().toString());
    }
    testTable.addReplaceCommit(instantTime, Option.empty(), Option.empty(), md);
  }

  private void addCleanWithEctr(String cleanInstantTime, String ectrTimestamp, Set<String> cleanedPartitions)
      throws IOException {
    HoodieActionInstant earliestInstantToRetain = ectrTimestamp == null
        ? null
        : new HoodieActionInstant(ectrTimestamp, HoodieTimeline.COMMIT_ACTION, HoodieInstant.State.COMPLETED.name());
    HoodieCleanerPlan plan = new HoodieCleanerPlan(
        earliestInstantToRetain,
        "",
        HoodieCleaningPolicy.KEEP_LATEST_COMMITS.name(),
        new HashMap<>(),
        CleanPlanV2MigrationHandler.VERSION,
        new HashMap<>(),
        new ArrayList<>(),
        Collections.emptyMap());
    Map<String, HoodieCleanPartitionMetadata> partitionMetadata = new HashMap<>();
    for (String p : cleanedPartitions) {
      partitionMetadata.put(p, HoodieCleanPartitionMetadata.newBuilder()
          .setPartitionPath(p)
          .setPolicy(HoodieCleaningPolicy.KEEP_LATEST_COMMITS.name())
          .setDeletePathPatterns(Collections.emptyList())
          .setSuccessDeleteFiles(Collections.singletonList(p + "/" + UUID.randomUUID() + ".parquet"))
          .setFailedDeleteFiles(Collections.emptyList())
          .build());
    }
    HoodieCleanMetadata cleanMetadata = new HoodieCleanMetadata(
        cleanInstantTime,
        0L,
        cleanedPartitions.size(),
        ectrTimestamp == null ? "" : ectrTimestamp,
        "",
        partitionMetadata,
        CleanPlanV2MigrationHandler.VERSION,
        Collections.emptyMap(),
        Collections.emptyMap());
    testTable.addClean(cleanInstantTime, plan, cleanMetadata);
  }

  private static Set<String> setOf(String... values) {
    return new java.util.HashSet<>(java.util.Arrays.asList(values));
  }
}
