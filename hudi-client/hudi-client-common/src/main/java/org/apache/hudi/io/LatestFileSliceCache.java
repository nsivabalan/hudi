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

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class LatestFileSliceCache {
  private static final Logger LOG = LoggerFactory.getLogger(LatestFileSliceCache.class);

  private static LoadingCache<Triple<String, String, String>, Option<FileSlice>> LATEST_FILE_SLICE_CACHE = null;
  private static Map<Triple<String, String, String>, Option<FileSlice>> LATEST_MERGED_FILE_SLICES = new ConcurrentHashMap<>();
  private static Set<String> instantTimes = new HashSet<>();

  public static LoadingCache<Triple<String, String, String>, Option<FileSlice>> getCache(TableFileSystemView.SliceView sliceView) {
    if (LATEST_FILE_SLICE_CACHE == null) {
      synchronized (LatestFileSliceCache.class) {
        LATEST_FILE_SLICE_CACHE = Caffeine.newBuilder()
            .weakValues()
            .maximumSize(100000)
            .expireAfterWrite(Duration.of(30, ChronoUnit.MINUTES))
            .build(new LatestFileSliceCacheLoader(sliceView, LATEST_MERGED_FILE_SLICES, instantTimes));
      }
    }
    return LATEST_FILE_SLICE_CACHE;
  }
}

class LatestFileSliceCacheLoader implements CacheLoader<Triple<String, String, String>, Option<FileSlice>> {
  private static final Logger LOG = LoggerFactory.getLogger(LatestFileSliceCacheLoader.class);
  private TableFileSystemView.SliceView sliceView;
  private Map<Triple<String, String, String>, Option<FileSlice>> latestMergedFileSlices;
  private Set<String> instantTimes;

  public LatestFileSliceCacheLoader(TableFileSystemView.SliceView sliceView,
                                    Map<Triple<String, String, String>, Option<FileSlice>> latestMergedFileSlices,
                                    Set<String> instantTimes) {
    this.sliceView = sliceView;
    this.latestMergedFileSlices = latestMergedFileSlices;
    this.instantTimes = instantTimes;
  }

  @Override
  public @Nullable Option<FileSlice> load(@NonNull Triple<String, String, String> key) throws Exception {
    if (instantTimes.contains(key.getRight())) {
      // should definitely be part of local cache.
      if (latestMergedFileSlices.containsKey(key)) {
        // return cached entry
        return latestMergedFileSlices.get(key);
      } else {
        LOG.warn("Instant found, but latest file slice not found " + key.getMiddle() + ", Entering waiting loop");
        long waitedSoFar = 0;
        while ((waitedSoFar < 1000 * 60 * 5) && !latestMergedFileSlices.containsKey(key)) {
          Thread.sleep(1000 * 30);
          waitedSoFar += 1000 * 30;
        }
        if (latestMergedFileSlices.containsKey(key)) {
          LOG.warn("Latest file slice populated in " + waitedSoFar + " ms for " + key.getMiddle());
          return latestMergedFileSlices.get(key);
        } else {
          // not reachable
          throw new HoodieException("Instant time {" + key.getRight() + "} is cached, but no file slice found for " + key.getLeft() + ", " + key.getMiddle()
              + ", even after waiting for " + waitedSoFar);
        }
      }
    }
    synchronized (LatestFileSliceCacheLoader.class) {
      // if not part of instant time, we need to load from FSV and populate the cache.
      // clear cache.
      instantTimes.clear();
      latestMergedFileSlices.clear();
      sliceView.getLatestMergedFileSlicesBeforeOrOn(key.getLeft(), key.getRight()).forEach(fileSlice -> {
        latestMergedFileSlices.put(Triple.of(key.getLeft(), fileSlice.getFileId(), key.getRight()), Option.of(fileSlice));
      });
      instantTimes.add(key.getRight());
      return latestMergedFileSlices.get(key);
    }
  }
}
