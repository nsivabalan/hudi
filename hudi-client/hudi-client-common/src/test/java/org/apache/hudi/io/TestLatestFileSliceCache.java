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
import org.apache.hudi.metadata.MetadataPartitionType;

import com.github.benmanes.caffeine.cache.LoadingCache;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestLatestFileSliceCache {

  private static final String FILEID1 = "fileId1";
  private static final String FILEID2 = "fileId2";
  private static final String FILEID3 = "fileId3";
  private static final String FILEID4 = "fileId4";
  private static final String FILEID5 = "fileId5";
  private static final String FILEID6 = "fileId6";
  private FileSlice fileSlice_p1_fileID1;
  private FileSlice fileSlice_p1_fileID2;
  private FileSlice fileSlice_p2_fileID3;
  private FileSlice fileSlice_p2_fileID4;
  private FileSlice fileSlice_p3_fileID5;
  private FileSlice fileSlice_p3_fileID6;

  private TableFileSystemView.SliceView sliceView;

  @BeforeEach
  public void setUp() {
    sliceView = mock(TableFileSystemView.SliceView.class);
    fileSlice_p1_fileID1 = new FileSlice(MetadataPartitionType.RECORD_INDEX.getPartitionPath(), "000111", FILEID1);
    when(sliceView.getLatestFileSlice(MetadataPartitionType.RECORD_INDEX.getPartitionPath(), FILEID1)).thenReturn(Option.of(fileSlice_p1_fileID1));
    fileSlice_p1_fileID2 = new FileSlice(MetadataPartitionType.RECORD_INDEX.getPartitionPath(), "000111", FILEID2);
    when(sliceView.getLatestFileSlice(MetadataPartitionType.RECORD_INDEX.getPartitionPath(), FILEID2)).thenReturn(Option.of(fileSlice_p1_fileID2));
    fileSlice_p2_fileID3 = new FileSlice(MetadataPartitionType.RECORD_INDEX.getPartitionPath(), "000111", FILEID3);
    when(sliceView.getLatestFileSlice(MetadataPartitionType.RECORD_INDEX.getPartitionPath(), FILEID3)).thenReturn(Option.of(fileSlice_p2_fileID3));
    fileSlice_p2_fileID4 = new FileSlice(MetadataPartitionType.RECORD_INDEX.getPartitionPath(), "000111", FILEID4);
    when(sliceView.getLatestFileSlice(MetadataPartitionType.RECORD_INDEX.getPartitionPath(), FILEID4)).thenReturn(Option.of(fileSlice_p2_fileID4));
    fileSlice_p3_fileID5 = new FileSlice(MetadataPartitionType.RECORD_INDEX.getPartitionPath(), "000111", FILEID5);
    when(sliceView.getLatestFileSlice(MetadataPartitionType.RECORD_INDEX.getPartitionPath(), FILEID5)).thenReturn(Option.of(fileSlice_p3_fileID5));
    fileSlice_p3_fileID6 = new FileSlice(MetadataPartitionType.RECORD_INDEX.getPartitionPath(), "000111", FILEID6);
    when(sliceView.getLatestFileSlice(MetadataPartitionType.RECORD_INDEX.getPartitionPath(), FILEID6)).thenReturn(Option.of(fileSlice_p3_fileID6));
  }

  /*@Test
  public void testLatestFileSliceCache() {
    LoadingCache<Triple<String, String, String>, Option<FileSlice>> latestFileSliceCache = LatestFileSliceCache.getCache(sliceView, "0001");
    for (int i=0;i < 3;i++) {
      assertEquals(Option.of(fileSlice_p1_fileID1), latestFileSliceCache.get(Triple.of(MetadataPartitionType.RECORD_INDEX.getPartitionPath(), FILEID1, "0001")));
      assertEquals(Option.of(fileSlice_p1_fileID2), latestFileSliceCache.get(Triple.of(MetadataPartitionType.RECORD_INDEX.getPartitionPath(), FILEID2, "0001")));
      assertEquals(Option.of(fileSlice_p2_fileID3), latestFileSliceCache.get(Triple.of(MetadataPartitionType.RECORD_INDEX.getPartitionPath(), FILEID3, "0001")));
      assertEquals(Option.of(fileSlice_p2_fileID4), latestFileSliceCache.get(Triple.of(MetadataPartitionType.RECORD_INDEX.getPartitionPath(), FILEID4, "0001")));
      assertEquals(Option.of(fileSlice_p3_fileID5), latestFileSliceCache.get(Triple.of(MetadataPartitionType.RECORD_INDEX.getPartitionPath(), FILEID5, "0001")));
      assertEquals(Option.of(fileSlice_p3_fileID6), latestFileSliceCache.get(Triple.of(MetadataPartitionType.RECORD_INDEX.getPartitionPath(), FILEID6, "0001")));
    }

    verify(sliceView, times(1)).getLatestFileSlice(MetadataPartitionType.RECORD_INDEX.getPartitionPath(), FILEID1);
    verify(sliceView, times(1)).getLatestFileSlice(MetadataPartitionType.RECORD_INDEX.getPartitionPath(), FILEID2);
    verify(sliceView, times(1)).getLatestFileSlice(MetadataPartitionType.RECORD_INDEX.getPartitionPath(), FILEID3);
    verify(sliceView, times(1)).getLatestFileSlice(MetadataPartitionType.RECORD_INDEX.getPartitionPath(), FILEID4);
    verify(sliceView, times(1)).getLatestFileSlice(MetadataPartitionType.RECORD_INDEX.getPartitionPath(), FILEID5);
    verify(sliceView, times(1)).getLatestFileSlice(MetadataPartitionType.RECORD_INDEX.getPartitionPath(), FILEID6);

    // if instant time changes, cached entry will be ignored
    latestFileSliceCache = LatestFileSliceCache.getCache(sliceView, "0002");
    for (int i=0;i < 3;i++) {
      assertEquals(Option.of(fileSlice_p1_fileID1), latestFileSliceCache.get(Triple.of(MetadataPartitionType.RECORD_INDEX.getPartitionPath(), FILEID1, "0002")));
      assertEquals(Option.of(fileSlice_p1_fileID2), latestFileSliceCache.get(Triple.of(MetadataPartitionType.RECORD_INDEX.getPartitionPath(), FILEID2, "0002")));
      assertEquals(Option.of(fileSlice_p2_fileID3), latestFileSliceCache.get(Triple.of(MetadataPartitionType.RECORD_INDEX.getPartitionPath(), FILEID3, "0002")));
      assertEquals(Option.of(fileSlice_p2_fileID4), latestFileSliceCache.get(Triple.of(MetadataPartitionType.RECORD_INDEX.getPartitionPath(), FILEID4, "0002")));
      assertEquals(Option.of(fileSlice_p3_fileID5), latestFileSliceCache.get(Triple.of(MetadataPartitionType.RECORD_INDEX.getPartitionPath(), FILEID5, "0002")));
      assertEquals(Option.of(fileSlice_p3_fileID6), latestFileSliceCache.get(Triple.of(MetadataPartitionType.RECORD_INDEX.getPartitionPath(), FILEID6, "0002")));
    }

    verify(sliceView, times(2)).getLatestFileSlice(MetadataPartitionType.RECORD_INDEX.getPartitionPath(), FILEID1);
    verify(sliceView, times(2)).getLatestFileSlice(MetadataPartitionType.RECORD_INDEX.getPartitionPath(), FILEID2);
    verify(sliceView, times(2)).getLatestFileSlice(MetadataPartitionType.RECORD_INDEX.getPartitionPath(), FILEID3);
    verify(sliceView, times(2)).getLatestFileSlice(MetadataPartitionType.RECORD_INDEX.getPartitionPath(), FILEID4);
    verify(sliceView, times(2)).getLatestFileSlice(MetadataPartitionType.RECORD_INDEX.getPartitionPath(), FILEID5);
    verify(sliceView, times(2)).getLatestFileSlice(MetadataPartitionType.RECORD_INDEX.getPartitionPath(), FILEID6);
  }*/
}
