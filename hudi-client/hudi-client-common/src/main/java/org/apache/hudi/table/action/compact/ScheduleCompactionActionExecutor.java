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

package org.apache.hudi.table.action.compact;

import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieCompactionException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.BaseActionExecutor;
import org.apache.hudi.table.action.compact.plan.generators.BaseHoodieCompactionPlanGenerator;
import org.apache.hudi.table.action.compact.plan.generators.HoodieCompactionPlanGenerator;
import org.apache.hudi.table.action.compact.plan.generators.HoodieLogCompactionPlanGenerator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.common.util.CollectionUtils.nonEmpty;
import static org.apache.hudi.common.util.ValidationUtils.checkArgument;

public class ScheduleCompactionActionExecutor<T, I, K, O> extends BaseActionExecutor<T, I, K, O, Option<HoodieCompactionPlan>> {

  private static final Logger LOG = LoggerFactory.getLogger(ScheduleCompactionActionExecutor.class);
  private WriteOperationType operationType;
  private final Option<Map<String, String>> extraMetadata;
  private BaseHoodieCompactionPlanGenerator planGenerator;

  public ScheduleCompactionActionExecutor(HoodieEngineContext context,
                                          HoodieWriteConfig config,
                                          HoodieTable<T, I, K, O> table,
                                          String instantTime,
                                          Option<Map<String, String>> extraMetadata,
                                          WriteOperationType operationType) {
    super(context, config, table, instantTime);
    this.extraMetadata = extraMetadata;
    this.operationType = operationType;
    checkArgument(operationType == WriteOperationType.COMPACT || operationType == WriteOperationType.LOG_COMPACT,
        "Only COMPACT and LOG_COMPACT is supported");
    initPlanGenerator(context, config, table);
  }

  private void initPlanGenerator(HoodieEngineContext context, HoodieWriteConfig config, HoodieTable<T, I, K, O> table) {
    if (WriteOperationType.COMPACT.equals(operationType)) {
      planGenerator = new HoodieCompactionPlanGenerator(table, context, config);
    } else {
      planGenerator = new HoodieLogCompactionPlanGenerator(table, context, config);
    }
  }

  @Override
  public Option<HoodieCompactionPlan> execute() {
    ValidationUtils.checkArgument(this.table.getMetaClient().getTableType() == HoodieTableType.MERGE_ON_READ,
        "Can only compact table of type " + HoodieTableType.MERGE_ON_READ + " and not "
            + this.table.getMetaClient().getTableType().name());
    if (!config.getWriteConcurrencyMode().supportsOptimisticConcurrencyControl()
        && !config.getFailedWritesCleanPolicy().isLazy()) {
      // TODO(yihua): this validation is removed for Java client used by kafka-connect.  Need to revisit this.
      if (config.getEngineType() == EngineType.SPARK) {
        // if there are inflight writes, their instantTime must not be less than that of compaction instant time
        Option<HoodieInstant> earliestInflightOpt = table.getActiveTimeline().getCommitsTimeline().filterPendingExcludingMajorAndMinorCompaction().firstInstant();
        if (earliestInflightOpt.isPresent() && !HoodieTimeline.compareTimestamps(earliestInflightOpt.get().getTimestamp(), HoodieTimeline.GREATER_THAN, instantTime)) {
          LOG.warn("Earliest write inflight instant time must be later than compaction time. Earliest :" + earliestInflightOpt.get()
              + ", Compaction scheduled at " + instantTime + ". Hence skipping to schedule compaction");
          return Option.empty();
        }
      }
      // Committed and pending compaction instants should have strictly lower timestamps
      List<HoodieInstant> conflictingInstants = table.getActiveTimeline()
          .getWriteTimeline().filterCompletedAndCompactionInstants().getInstantsAsStream()
          .filter(instant -> HoodieTimeline.compareTimestamps(
              instant.getTimestamp(), HoodieTimeline.GREATER_THAN_OR_EQUALS, instantTime))
          .collect(Collectors.toList());
      ValidationUtils.checkArgument(conflictingInstants.isEmpty(),
          "Following instants have timestamps >= compactionInstant (" + instantTime + ") Instants :"
              + conflictingInstants);
    }

    HoodieCompactionPlan plan = scheduleCompaction();
    Option<HoodieCompactionPlan> option = Option.empty();
    if (plan != null && nonEmpty(plan.getOperations())) {
      extraMetadata.ifPresent(plan::setExtraMetadata);
      table.validateForLatestTimestamp(instantTime, WriteOperationType.COMPACT.name());
      try {
        if (operationType.equals(WriteOperationType.COMPACT)) {
          HoodieInstant compactionInstant = new HoodieInstant(HoodieInstant.State.REQUESTED,
              HoodieTimeline.COMPACTION_ACTION, instantTime);
          table.getActiveTimeline().saveToCompactionRequested(compactionInstant,
              TimelineMetadataUtils.serializeCompactionPlan(plan));
        } else {
          HoodieInstant logCompactionInstant = new HoodieInstant(HoodieInstant.State.REQUESTED,
              HoodieTimeline.LOG_COMPACTION_ACTION, instantTime);
          table.getActiveTimeline().saveToLogCompactionRequested(logCompactionInstant,
              TimelineMetadataUtils.serializeCompactionPlan(plan));
        }
      } catch (IOException ioe) {
        throw new HoodieIOException("Exception scheduling compaction", ioe);
      }
      option = Option.of(plan);
    }

    return option;
  }

  @Nullable
  private HoodieCompactionPlan scheduleCompaction() {
    LOG.info("Checking if compaction needs to be run on " + config.getBasePath());
    // judge if we need to compact according to num delta commits and time elapsed
    boolean compactable = needCompact(config.getInlineCompactTriggerStrategy());
    if (compactable) {
      LOG.info("Generating compaction plan for merge on read table " + config.getBasePath());
      try {
        context.setJobStatus(this.getClass().getSimpleName(), "Compaction: generating compaction plan");
        return planGenerator.generateCompactionPlan();
      } catch (IOException e) {
        throw new HoodieCompactionException("Could not schedule compaction " + config.getBasePath(), e);
      }
    }
    return new HoodieCompactionPlan();
  }

  private Option<Pair<Integer, String>> getLatestDeltaCommitInfo() {
    Option<Pair<HoodieTimeline, HoodieInstant>> deltaCommitsInfo =
        CompactionUtils.getDeltaCommitsSinceLatestCompaction(table.getActiveTimeline());
    if (deltaCommitsInfo.isPresent()) {
      return Option.of(Pair.of(
          deltaCommitsInfo.get().getLeft().countInstants(),
          deltaCommitsInfo.get().getRight().getTimestamp()));
    }
    return Option.empty();
  }

  private Option<Pair<Integer, String>> getLatestDeltaCommitInfoSinceLastCompactionRequest() {
    Option<Pair<HoodieTimeline, HoodieInstant>> deltaCommitsInfo =
          CompactionUtils.getDeltaCommitsSinceLatestCompactionRequest(table.getActiveTimeline());
    if (deltaCommitsInfo.isPresent()) {
      return Option.of(Pair.of(
            deltaCommitsInfo.get().getLeft().countInstants(),
            deltaCommitsInfo.get().getRight().getTimestamp()));
    }
    return Option.empty();
  }

  private boolean needCompact(CompactionTriggerStrategy compactionTriggerStrategy) {
    boolean compactable;
    // get deltaCommitsSinceLastCompaction and lastCompactionTs
    Option<Pair<Integer, String>> latestDeltaCommitInfoOption = getLatestDeltaCommitInfo();
    if (!latestDeltaCommitInfoOption.isPresent()) {
      return false;
    }
    Pair<Integer, String> latestDeltaCommitInfo = latestDeltaCommitInfoOption.get();
    if (WriteOperationType.LOG_COMPACT.equals(operationType)) {
      return true;
    }
    int inlineCompactDeltaCommitMax = config.getInlineCompactDeltaCommitMax();
    int inlineCompactDeltaSecondsMax = config.getInlineCompactDeltaSecondsMax();
    switch (compactionTriggerStrategy) {
      case NUM_COMMITS:
        compactable = inlineCompactDeltaCommitMax <= latestDeltaCommitInfo.getLeft();
        if (compactable) {
          LOG.info(String.format("The delta commits >= %s, trigger compaction scheduler.", inlineCompactDeltaCommitMax));
        }
        break;
      case NUM_COMMITS_AFTER_LAST_REQUEST:
        latestDeltaCommitInfoOption = getLatestDeltaCommitInfoSinceLastCompactionRequest();

        if (!latestDeltaCommitInfoOption.isPresent()) {
          return false;
        }
        latestDeltaCommitInfo = latestDeltaCommitInfoOption.get();
        compactable = inlineCompactDeltaCommitMax <= latestDeltaCommitInfo.getLeft();
        if (compactable) {
          LOG.info(String.format("The delta commits >= %s since the last compaction request, trigger compaction scheduler.", inlineCompactDeltaCommitMax));
        }
        break;
      case TIME_ELAPSED:
        compactable = inlineCompactDeltaSecondsMax <= parsedToSeconds(instantTime) - parsedToSeconds(latestDeltaCommitInfo.getRight());
        if (compactable) {
          LOG.info(String.format("The elapsed time >=%ss, trigger compaction scheduler.", inlineCompactDeltaSecondsMax));
        }
        break;
      case NUM_OR_TIME:
        compactable = inlineCompactDeltaCommitMax <= latestDeltaCommitInfo.getLeft()
            || inlineCompactDeltaSecondsMax <= parsedToSeconds(instantTime) - parsedToSeconds(latestDeltaCommitInfo.getRight());
        if (compactable) {
          LOG.info(String.format("The delta commits >= %s or elapsed_time >=%ss, trigger compaction scheduler.", inlineCompactDeltaCommitMax,
              inlineCompactDeltaSecondsMax));
        }
        break;
      case NUM_AND_TIME:
        compactable = inlineCompactDeltaCommitMax <= latestDeltaCommitInfo.getLeft()
            && inlineCompactDeltaSecondsMax <= parsedToSeconds(instantTime) - parsedToSeconds(latestDeltaCommitInfo.getRight());
        if (compactable) {
          LOG.info(String.format("The delta commits >= %s and elapsed_time >=%ss, trigger compaction scheduler.", inlineCompactDeltaCommitMax,
              inlineCompactDeltaSecondsMax));
        }
        break;
      default:
        throw new HoodieCompactionException("Unsupported compaction trigger strategy: " + config.getInlineCompactTriggerStrategy());
    }
    return compactable;
  }

  private Long parsedToSeconds(String time) {
    return HoodieActiveTimeline.parseDateFromInstantTimeSafely(time).orElseThrow(() -> new HoodieCompactionException("Failed to parse timestamp " + time))
            .getTime() / 1000;
  }
}
