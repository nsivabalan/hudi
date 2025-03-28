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

package org.apache.hudi.cli.utils;

import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineUtils;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.List;

/**
 * Utilities related to commit operation.
 */
public class CommitUtil {

  public static long countNewRecords(HoodieTableMetaClient metaClient, List<String> commitsToCatchup) throws IOException {
    long totalNew = 0;
    HoodieTimeline timeline = metaClient.reloadActiveTimeline().getCommitAndReplaceTimeline().filterCompletedInstants();
    for (String commit : commitsToCatchup) {
      HoodieInstant instant = metaClient.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, commit);
      HoodieCommitMetadata c = timeline.readCommitMetadata(instant);
      totalNew += c.fetchTotalRecordsWritten() - c.fetchTotalUpdateRecordsWritten();
    }
    return totalNew;
  }

  public static String getTimeDaysAgo(int numberOfDays) {
    Date date = Date.from(ZonedDateTime.now().minusDays(numberOfDays).toInstant());
    return TimelineUtils.formatDate(date);
  }
}
