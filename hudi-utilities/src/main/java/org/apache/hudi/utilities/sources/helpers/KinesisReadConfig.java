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

package org.apache.hudi.utilities.sources.helpers;

import org.apache.hudi.utilities.config.KinesisSourceConfig;

import java.io.Serializable;

/**
 * Serializable configuration for Kinesis reads, used in Spark closures to avoid
 * capturing non-serializable KinesisOffsetGen.
 */
public class KinesisReadConfig implements Serializable {

  private static final long serialVersionUID = 1L;

  private final String streamName;
  private final String region;
  private final String endpointUrl; // null if not set
  private final String accessKey; // null if not set
  private final String secretKey; // null if not set
  private final KinesisSourceConfig.KinesisStartingPositionStrategy startingPosition;
  private final boolean shouldAddMetaFields;
  private final boolean enableDeaggregation;
  private final int maxRecordsPerRequest;
  private final long intervalMilliSeconds;
  private final long maxRecordsPerShard;
  private final long retryInitialIntervalMs;
  private final long retryMaxIntervalMs;
  private final long throttleTimeoutMs;

  public KinesisReadConfig(String streamName, String region, String endpointUrl,
      String accessKey, String secretKey,
      KinesisSourceConfig.KinesisStartingPositionStrategy startingPosition,
      boolean shouldAddMetaFields, boolean enableDeaggregation,
      int maxRecordsPerRequest, long intervalMilliSeconds, long maxRecordsPerShard,
      long retryInitialIntervalMs, long retryMaxIntervalMs, long throttleTimeoutMs) {
    this.streamName = streamName;
    this.region = region;
    this.endpointUrl = endpointUrl;
    this.accessKey = accessKey;
    this.secretKey = secretKey;
    this.startingPosition = startingPosition;
    this.shouldAddMetaFields = shouldAddMetaFields;
    this.enableDeaggregation = enableDeaggregation;
    this.maxRecordsPerRequest = maxRecordsPerRequest;
    this.intervalMilliSeconds = intervalMilliSeconds;
    this.maxRecordsPerShard = maxRecordsPerShard;
    this.retryInitialIntervalMs = retryInitialIntervalMs;
    this.retryMaxIntervalMs = retryMaxIntervalMs;
    this.throttleTimeoutMs = throttleTimeoutMs;
  }

  public String getStreamName() {
    return streamName;
  }

  public String getRegion() {
    return region;
  }

  public String getEndpointUrl() {
    return endpointUrl;
  }

  public String getAccessKey() {
    return accessKey;
  }

  public String getSecretKey() {
    return secretKey;
  }

  public KinesisSourceConfig.KinesisStartingPositionStrategy getStartingPosition() {
    return startingPosition;
  }

  public boolean shouldAddMetaFields() {
    return shouldAddMetaFields;
  }

  public boolean isDeaggregationEnabled() {
    return enableDeaggregation;
  }

  public int getMaxRecordsPerRequest() {
    return maxRecordsPerRequest;
  }

  public long getIntervalMilliSeconds() {
    return intervalMilliSeconds;
  }

  public long getMaxRecordsPerShard() {
    return maxRecordsPerShard;
  }

  public long getRetryInitialIntervalMs() {
    return retryInitialIntervalMs;
  }

  public long getRetryMaxIntervalMs() {
    return retryMaxIntervalMs;
  }

  public long getThrottleTimeoutMs() {
    return throttleTimeoutMs;
  }
}
