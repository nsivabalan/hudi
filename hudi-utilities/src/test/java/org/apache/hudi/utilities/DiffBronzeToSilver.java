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

package org.apache.hudi.utilities;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

public class DiffBronzeToSilver {

  public static class DiffStats {
    public long numTotalInBronze;
    public long numLatestInBronze;
    public long numTotalInSilver;
    public long numDups;
    public long numDupsWithSamePartitionAndOrdering;
    // public long numDupsIdenticalExceptCommitSeqNo; // skip this due to heavy operation
    public long numDupsWithDifferentPartition;
    public long numDupsWithDifferentOrder;
    public long maxDupCount;
    public String dedupBackupPath;
    public long numMissingOrStaled;
    public long numMissing;
    public String earliestMissing;
    public long numStaled;
    public String earliestStaled;
    public long numBackfilling;
    public String backfillOpCounts;
    public String stagedBackfillDataPath;

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("DiffStats:\n");
      for (Field f : getClass().getDeclaredFields()) {
        f.setAccessible(true);
        try {
          sb.append(String.format("%s=%s\n", f.getName(), f.get(this)));
        } catch (IllegalAccessException e) {
          throw new RuntimeException(e);
        }
      }
      return sb.toString();
    }

    public Map<String, String> toMap() {
      Map<String, String> map = new HashMap<>();
      for (Field f : getClass().getDeclaredFields()) {
        f.setAccessible(true);
        try {
          map.put(f.getName(), String.valueOf(f.get(this)));
        } catch (Exception e) {
          throw new RuntimeException(e);
        }

      }
      return map;
    }
  }
}

