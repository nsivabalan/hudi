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
  void noCleanInstant_returnsOnlyPartitionsWithReplacedFileIds() throws Exception {
    // Plain commits should be ignored — they don't replace any file IDs.
    testTable.addCommit("001", Option.of(commitMetadataFor("p_old_a")));
    testTable.addCommit("002", Option.of(commitMetadataFor("p_old_b")));
    // For replace commits, only the replaced-fileId side counts; the new-write side does not.
    addReplaceCommit("003", Collections.singleton("p_replaced_x"), Collections.singleton("p_new_x"));
    addDeletePartitionsCommit("004", Collections.singleton("p_deleted_y"));

    Set<String> partitions = runTool();

    assertEquals(setOf("p_replaced_x", "p_deleted_y"), partitions);
  }

  @Test
  void cleanWithValidEctr_returnsOnlyReplacedPartitionsAtOrAfterEctr() throws Exception {
    testTable.addCommit("001", Option.of(commitMetadataFor("p_pre_clean_a")));
    addReplaceCommit("002", Collections.singleton("p_pre_clean_b"), Collections.singleton("p_pre_clean_c"));
    // Clean records ECTR = "010" => only replace-fileId partitions at/after 010 should be included.
    addCleanWithEctr("005", "010");
    testTable.addCommit("011", Option.of(commitMetadataFor("p_post_clean_a")));
    addReplaceCommit("012", Collections.singleton("p_replaced_post"), Collections.singleton("p_new_post"));
    addDeletePartitionsCommit("013", Collections.singleton("p_deleted_post"));

    Set<String> partitions = runTool();

    assertEquals(setOf("p_replaced_post", "p_deleted_post"), partitions);
  }

  @Test
  void cleanWithEmptyEctr_returnsAllReplacedPartitions() throws Exception {
    testTable.addCommit("001", Option.of(commitMetadataFor("p_a")));
    addReplaceCommit("002", Collections.singleton("p_replaced"), Collections.singleton("p_new"));
    addDeletePartitionsCommit("003", Collections.singleton("p_deleted"));
    // Clean exists, but its plan has no earliestInstantToRetain — mirrors the
    // CleanerUtils.getEarliestCommitToRetain == empty case.
    addCleanWithEctr("004", null);

    Set<String> partitions = runTool();

    assertEquals(setOf("p_replaced", "p_deleted"), partitions);
  }

  @Test
  void replaceInstantsStrictlyBeforeEctr_areExcluded() throws Exception {
    addReplaceCommit("001", Collections.singleton("p_before"), Collections.emptySet());
    addCleanWithEctr("005", "002");
    addReplaceCommit("002", Collections.singleton("p_at_ectr"), Collections.emptySet());
    addReplaceCommit("003", Collections.singleton("p_after_ectr"), Collections.emptySet());

    Set<String> partitions = runTool();

    assertTrue(partitions.contains("p_at_ectr"));
    assertTrue(partitions.contains("p_after_ectr"));
    assertEquals(setOf("p_at_ectr", "p_after_ectr"), partitions);
  }

  @Test
  void replaceCommitWithNoReplacedFileIds_isIgnored() throws Exception {
    // Insert-overwrite-table-style or pure-write replace commits with empty replace map shouldn't show up.
    addReplaceCommit("001", Collections.emptySet(), Collections.singleton("p_new_only"));
    addReplaceCommit("002", Collections.singleton("p_replaced"), Collections.emptySet());

    Set<String> partitions = runTool();

    assertEquals(setOf("p_replaced"), partitions);
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

  private void addCleanWithEctr(String cleanInstantTime, String ectrTimestamp) throws IOException {
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
    HoodieCleanMetadata cleanMetadata = new HoodieCleanMetadata(
        cleanInstantTime,
        0L,
        0,
        ectrTimestamp == null ? "" : ectrTimestamp,
        "",
        Collections.emptyMap(),
        CleanPlanV2MigrationHandler.VERSION,
        Collections.emptyMap(),
        Collections.emptyMap());
    testTable.addClean(cleanInstantTime, plan, cleanMetadata);
  }

  private static Set<String> setOf(String... values) {
    return new java.util.HashSet<>(java.util.Arrays.asList(values));
  }
}
