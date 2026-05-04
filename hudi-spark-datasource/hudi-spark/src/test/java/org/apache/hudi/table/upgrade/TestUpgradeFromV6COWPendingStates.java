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

package org.apache.hudi.table.upgrade;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteClientTestUtils;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.testutils.HoodieClientTestBase;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Validates that the Hudi 1.1 binary correctly handles a Hudi 0.14 (table version 6) COW
 * table with pending uncommitted writes. Fixture: {@code cow_pending_states.zip}; see the
 * fixtures README for the full timeline structure.
 */
class TestUpgradeFromV6COWPendingStates extends HoodieClientTestBase {

  @BeforeEach
  public void setUp() throws Exception {
    initSparkContexts();
    initTestDataGenerator();
  }

  @AfterEach
  public void tearDown() throws Exception {
    cleanupResources();
  }

  @Test
  public void testPendingCOWWriteIsRolledBackOnNewCommit() throws Exception {
    HoodieTestUtils.extractZipToDirectory(
        "upgrade-downgrade-fixtures/cow_pending_states.zip", tempDir, getClass());
    basePath = tempDir.resolve("cow_pending_states").toString();
    metaClient = HoodieTableMetaClient.builder()
        .setConf(context.getStorageConf().newInstance())
        .setBasePath(basePath)
        .build();

    assertEquals(HoodieTableVersion.SIX, metaClient.getTableConfig().getTableVersion());
    assertEquals(2, metaClient.getCommitsTimeline().filterCompletedInstants().countInstants());
    assertTrue(metaClient.getActiveTimeline().filterInflights().containsInstant("003"));

    Dataset<Row> initialSnapshot = readTable();
    assertEquals(150, initialSnapshot.count(), "001 (100) + 002 (50)");
    assertEquals(toSet("001", "002"), commitTimesIn(initialSnapshot));

    // small.file.limit=0: prevents the new insert from packing records into 001's file groups,
    // which would let auto-clean delete 001's base files that the pending clustering plan
    // (005) references.
    //
    // column_stats disabled: the 1.1 Spark default enables it, but the v6 fixture's MDT only
    // has the `files` partition. The v6 MDT writer's pending-ops guard rejects the bootstrap
    // of the missing column_stats partition, and as a side-effect halts maintenance of the
    // existing `files` partition too — so the new commit's files would be invisible to MDT
    // readers.
    HoodieWriteConfig cfg = getConfigBuilder()
        .withAutoUpgradeVersion(false)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().withMetadataIndexColumnStats(false).build())
        .withProps(Collections.singletonMap("hoodie.parquet.small.file.limit", "0"))
        .build();

    String newCommit = "007";
    List<WriteStatus> writeStatuses;
    try (SparkRDDWriteClient client = getHoodieWriteClient(cfg)) {
      WriteClientTestUtils.startCommitWithTime(client, newCommit);
      // Materialise once via .collect() then rebuild an RDD for commit(). Passing the same
      // insert RDD to both client.commit() and .collect() runs the insert twice and orphans
      // a second set of parquet files.
      writeStatuses = client.insert(
          jsc.parallelize(dataGen.generateInserts(newCommit, 20), 2), newCommit).collect();
      client.commit(newCommit, jsc.parallelize(writeStatuses, 1));
    }
    assertNoWriteErrors(writeStatuses);

    metaClient = HoodieTableMetaClient.reload(metaClient);

    assertTrue(metaClient.getActiveTimeline().getCommitsTimeline().filterInflights().empty());

    List<String> completedTimes = metaClient.getCommitsTimeline().filterCompletedInstants()
        .getInstantsAsStream()
        .map(HoodieInstant::requestedTime)
        .collect(Collectors.toList());
    assertFalse(completedTimes.contains("003"));
    assertTrue(completedTimes.contains(newCommit));
    assertEquals(2, completedTimes.size(), "001 + new commit; 002 was rolled back by 004");

    // Two rollbacks: pre-existing 004.rollback.requested completes via getPendingRollbackInfos,
    // and EAGER policy creates a new rollback for the inflight 003.
    assertEquals(2, metaClient.getActiveTimeline().getRollbackTimeline()
        .filterCompletedInstants().countInstants());

    assertEquals(1, metaClient.getActiveTimeline().getCleanerTimeline()
        .filterCompletedInstants().countInstants(), "pending clean 006 completes during commit");

    // Clustering instants are excluded from EAGER rollback.
    assertEquals(1, metaClient.getActiveTimeline().filterPendingReplaceOrClusteringTimeline().countInstants());

    Dataset<Row> postCommitSnapshot = readTable();
    assertEquals(120, postCommitSnapshot.count(), "001 (100) + new commit (20)");
    Set<String> postCommitCommitTimes = commitTimesIn(postCommitSnapshot);
    assertEquals(toSet("001", newCommit), postCommitCommitTimes);
    assertFalse(postCommitCommitTimes.contains("002"));
    assertFalse(postCommitCommitTimes.contains("003"));

    // Execute the pending clustering left over from 0.14.
    try (SparkRDDWriteClient client = getHoodieWriteClient(cfg)) {
      HoodieWriteMetadata<JavaRDD<WriteStatus>> clusterMetadata = client.cluster("005", true);
      assertNoWriteErrors(clusterMetadata.getWriteStatuses().collect());
    }

    metaClient = HoodieTableMetaClient.reload(metaClient);

    long completedClusteringCount = metaClient.getActiveTimeline()
        .filterCompletedInstants()
        .filter(i -> i.getAction().equals(HoodieTimeline.REPLACE_COMMIT_ACTION))
        .countInstants();
    assertEquals(1, completedClusteringCount);
    assertEquals(0, metaClient.getActiveTimeline().filterPendingReplaceOrClusteringTimeline().countInstants());

    Dataset<Row> postClusteringSnapshot = readTable();
    assertEquals(120, postClusteringSnapshot.count(), "clustering preserves record count");
  }

  private Dataset<Row> readTable() {
    return sqlContext.read().format("hudi").load(basePath);
  }

  private Set<String> commitTimesIn(Dataset<Row> snapshot) {
    return snapshot.select("_hoodie_commit_time")
        .distinct()
        .collectAsList()
        .stream()
        .map(row -> row.getString(0))
        .collect(Collectors.toSet());
  }

  private static Set<String> toSet(String... values) {
    return java.util.Arrays.stream(values).collect(Collectors.toSet());
  }
}
