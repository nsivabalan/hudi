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

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * MOR counterpart to {@link TestUpgradeFromV6COWPendingStates}: validates 1.1's handling of a
 * Hudi 0.14 (table version 6) MOR table with pending uncommitted writes. Fixture:
 * {@code mor_pending_states.zip}.
 */
public class TestUpgradeFromV6MORPendingStates extends HoodieClientTestBase {

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
  public void testPendingMORWriteIsRolledBackOnNewCommit() throws Exception {
    HoodieTestUtils.extractZipToDirectory(
        "upgrade-downgrade-fixtures/mor_pending_states.zip", tempDir, getClass());
    basePath = tempDir.resolve("mor_pending_states").toString();
    metaClient = HoodieTableMetaClient.builder()
        .setConf(context.getStorageConf().newInstance())
        .setBasePath(basePath)
        .build();

    assertEquals(HoodieTableVersion.SIX, metaClient.getTableConfig().getTableVersion());
    assertEquals(3, metaClient.getCommitsTimeline().filterCompletedInstants().countInstants());
    assertTrue(metaClient.getActiveTimeline().filterInflights().containsInstant("004"));
    assertEquals(1, metaClient.getActiveTimeline().filterPendingCompactionTimeline().countInstants());

    // 002/003 upsert the same keys 001 inserted, so the count stays at 100.
    Dataset<Row> initialSnapshot = readTable();
    assertEquals(100, initialSnapshot.count());
    assertFalse(commitTimesIn(initialSnapshot).contains("004"), "004 is inflight");

    // column_stats disabled: see TestUpgradeFromV6PendingStates for the v6 MDT bootstrap-guard
    // workaround.
    HoodieWriteConfig cfg = getConfigBuilder()
        .withAutoUpgradeVersion(false)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().withMetadataIndexColumnStats(false).build())
        .build();

    String newCommit = "007";
    List<WriteStatus> writeStatuses;
    try (SparkRDDWriteClient client = getHoodieWriteClient(cfg)) {
      WriteClientTestUtils.startCommitWithTime(client, newCommit);
      // generateInserts (not generateUpdates): a fresh dataGen has no memory of the fixture's
      // record keys. Upsert of brand-new keys behaves as an insert for record-count purposes.
      // Materialise once via .collect() then rebuild an RDD for commit() to avoid running
      // upsert twice and orphaning a second set of files.
      writeStatuses = client.upsert(
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
    assertFalse(completedTimes.contains("004"));
    assertTrue(completedTimes.contains(newCommit));
    assertEquals(4, completedTimes.size(), "001, 002, 003 + new commit");

    assertEquals(1, metaClient.getActiveTimeline().getRollbackTimeline()
        .filterCompletedInstants().countInstants(), "EAGER rollback of inflight 004");

    assertEquals(1, metaClient.getActiveTimeline().getCleanerTimeline()
        .filterCompletedInstants().countInstants(), "pending clean 006 completes during commit");

    // Compaction instants are excluded from EAGER rollback.
    assertEquals(1, metaClient.getActiveTimeline().filterPendingCompactionTimeline().countInstants());

    Dataset<Row> postCommitSnapshot = readTable();
    assertEquals(120, postCommitSnapshot.count(), "100 existing + 20 new keys");
    Set<String> postCommitCommitTimes = commitTimesIn(postCommitSnapshot);
    assertTrue(postCommitCommitTimes.contains(newCommit));
    assertFalse(postCommitCommitTimes.contains("004"));

    // shouldComplete=true so compact() commits the compaction (REQUESTED → INFLIGHT → COMMIT).
    // The single-arg overload would leave it INFLIGHT.
    try (SparkRDDWriteClient client = getHoodieWriteClient(cfg)) {
      HoodieWriteMetadata<JavaRDD<WriteStatus>> compactionMetadata = client.compact("005", true);
      assertNoWriteErrors(compactionMetadata.getWriteStatuses().collect());
    }

    metaClient = HoodieTableMetaClient.reload(metaClient);

    long completedCompactionCount = metaClient.getActiveTimeline()
        .filterCompletedInstants()
        .filter(i -> i.getAction().equals(HoodieTimeline.COMMIT_ACTION) && i.requestedTime().equals("005"))
        .countInstants();
    assertEquals(1, completedCompactionCount, "compaction lands on the timeline as a .commit");
    assertEquals(0, metaClient.getActiveTimeline().filterPendingCompactionTimeline().countInstants());

    Dataset<Row> postCompactionSnapshot = readTable();
    assertEquals(120, postCompactionSnapshot.count(), "compaction preserves record count");
  }

  private Dataset<Row> readTable() {
    return sqlContext.read().format("hudi")
        .option("hoodie.datasource.query.type", "snapshot")
        .load(basePath);
  }

  private Set<String> commitTimesIn(Dataset<Row> snapshot) {
    return snapshot.select("_hoodie_commit_time")
        .distinct()
        .collectAsList()
        .stream()
        .map(row -> row.getString(0))
        .collect(Collectors.toSet());
  }
}
