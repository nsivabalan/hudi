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
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.common.util.collection.Triple;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

public class LatestFileSliceCache {

  public static LoadingCache<Triple<String, String, String>, Option<FileSlice>> LATEST_FILE_SLICE_CACHE = null;

  public static LoadingCache<Triple<String, String, String>, Option<FileSlice>> getCache(TableFileSystemView.SliceView sliceView) {
    if (LATEST_FILE_SLICE_CACHE == null) {
      synchronized (LatestFileSliceCache.class) {
        LATEST_FILE_SLICE_CACHE = Caffeine.newBuilder()
            .weakValues()
            .maximumSize(100000)
            .expireAfterWrite(Duration.of(30, ChronoUnit.MINUTES))
            .build(new LatestFileSliceCacheLoader(sliceView));
      }
    }
    return LATEST_FILE_SLICE_CACHE;
  }
}

class LatestFileSliceCacheLoader implements CacheLoader<Triple<String, String, String>, Option<FileSlice>> {

  private TableFileSystemView.SliceView sliceView;

  public LatestFileSliceCacheLoader(TableFileSystemView.SliceView sliceView) {
    this.sliceView = sliceView;
  }

  @Override
  public @Nullable Option<FileSlice> load(@NonNull Triple<String, String, String> key) throws Exception {
    return sliceView.getLatestFileSlice(key.getLeft(), key.getMiddle());
  }
}
