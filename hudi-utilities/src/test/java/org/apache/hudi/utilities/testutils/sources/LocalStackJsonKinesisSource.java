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

package org.apache.hudi.utilities.testutils.sources;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.utilities.ingestion.HoodieIngestionMetrics;
import org.apache.hudi.utilities.sources.JsonKinesisSource;
import org.apache.hudi.utilities.streamer.StreamContext;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

/**
 * Test-only subclass of {@link JsonKinesisSource} that works around a LocalStack quirk:
 * LocalStack returns {@code Long.MAX_VALUE} as the ending sequence number for closed shards,
 * whereas real AWS returns the actual last sequence number. Storing the sentinel verbatim in the
 * checkpoint would poison downstream {@code lastSeq >= endSeq} comparisons.
 *
 * <p>Handling:
 * <ul>
 *   <li>With a valid lastSeq: replace sentinel with lastSeq so {@code lastSeq >= endSeq}
 *       correctly identifies the shard as fully consumed on the next run.</li>
 *   <li>Without a valid lastSeq (closed shard first seen with no records read): drop endSeq
 *       (return null) so the shard is omitted from the checkpoint rather than stored with
 *       sentinel. The shard will be re-listed on the next run and handled then.</li>
 * </ul>
 */
public class LocalStackJsonKinesisSource extends JsonKinesisSource {

  /** LocalStack returns Long.MAX_VALUE for closed shards' endingSequenceNumber; real AWS returns actual value. */
  public static final String LOCALSTACK_END_SEQ_SENTINEL = "9223372036854775807";

  public LocalStackJsonKinesisSource(TypedProperties props, JavaSparkContext sparkContext,
      SparkSession sparkSession, HoodieIngestionMetrics metrics, StreamContext streamContext) {
    super(props, sparkContext, sparkSession, metrics, streamContext);
  }

  @Override
  protected String normalizeEndSeq(String lastSeq, String endSeq) {
    if (LOCALSTACK_END_SEQ_SENTINEL.equals(endSeq)) {
      return (lastSeq != null && !lastSeq.isEmpty()) ? lastSeq : null;
    }
    return endSeq;
  }
}
