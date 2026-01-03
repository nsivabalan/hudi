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

package org.apache.hudi.io;

import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.table.view.TableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Triple;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.metadata.MetadataPartitionType;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.atomic.AtomicReference;

public class LatestFileSliceCache {
  private static final Logger LOG = LoggerFactory.getLogger(LatestFileSliceCache.class);

  private static String RLI_PARTITION_PATH = MetadataPartitionType.RECORD_INDEX.getPartitionPath();
  private static AtomicReference<Cache<Triple<String, String, String>, Option<FileSlice>>> LATEST_FILE_SLICE_CACHE = null;
  private static String INSTANT_TIME_CACHED = null;

  public static Cache<Triple<String, String, String>, Option<FileSlice>> getCache(TableFileSystemView.SliceView sliceView, String instantTime) {
    if (LATEST_FILE_SLICE_CACHE == null || INSTANT_TIME_CACHED == null || (!INSTANT_TIME_CACHED.equals(instantTime))) {
      synchronized (LatestFileSliceCache.class) {
        if (LATEST_FILE_SLICE_CACHE == null || INSTANT_TIME_CACHED == null || (!INSTANT_TIME_CACHED.equals(instantTime))) {
          LOG.warn("Instantiating new LATEST_FILE_SLICE_CACHE");
          LATEST_FILE_SLICE_CACHE = new AtomicReference<>(Caffeine.newBuilder()
              .maximumSize(100000)
              .expireAfterWrite(Duration.of(360, ChronoUnit.MINUTES))
              .build());
          LOG.warn("Populating entries into Latest file slice cache with instant time " + instantTime + " : Started ");
          // populate cache w/ latest file slice for all file groups
          sliceView.getLatestMergedFileSlicesBeforeOrOn(RLI_PARTITION_PATH, instantTime).forEach(fileSlice -> {
            LOG.warn("     " + RLI_PARTITION_PATH + ", file slice for "+ fileSlice.getFileId() +" being added to cache");
            LATEST_FILE_SLICE_CACHE.get().put(Triple.of(RLI_PARTITION_PATH, fileSlice.getFileId(), instantTime), Option.of(fileSlice));
          });
          INSTANT_TIME_CACHED = instantTime;
          LOG.warn("Populating entries into Latest file slice cache with instant time " + instantTime + " : Completed. Total entries " + LATEST_FILE_SLICE_CACHE.get().estimatedSize());
        } else {
          LOG.warn("Within sync block. but looks like already some other task populated the entries. Skipping to populate entries. Total entries " + LATEST_FILE_SLICE_CACHE.get().estimatedSize());
        }
      }
    }
    return LATEST_FILE_SLICE_CACHE.get();
  }
}