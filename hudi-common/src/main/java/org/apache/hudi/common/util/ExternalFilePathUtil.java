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

package org.apache.hudi.common.util;

import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import java.util.Collection;
import java.util.Collections;

/**
 * Utility methods for handling externally created files.
 */
public class ExternalFilePathUtil {
  // Suffix acts as a marker when appended to a file path that the path was created by an external system and not a Hudi writer.
  private static final String EXTERNAL_FILE_SUFFIX = "_hudiext";

  /**
   * Appends the commit time and external file marker to the file path. Hudi relies on the commit time in the file name for properly generating views of the files in a table.
   * @param filePath The original file path
   * @param commitTime The time of the commit that added this file to the table
   * @return The file path with this additional information appended
   */
  public static String appendCommitTimeAndExternalFileMarker(String filePath, String commitTime) {
    return filePath + "_" + commitTime + EXTERNAL_FILE_SUFFIX;
  }

  /**
   * Checks if the file name was created by an external system by checking for the external file marker at the end of the file name.
   * @param fileName The file name
   * @return True if the file was created by an external system, false otherwise
   */
  public static boolean isExternallyCreatedFile(String fileName) {
    return fileName.endsWith(EXTERNAL_FILE_SUFFIX);
  }

  /**
   * Checks if the commit metadata contains any external files (files with _hudiext suffix).
   *
   * @param commitMetadata The commit metadata to check
   * @return true if any write stat contains external files, false otherwise
   */
  public static boolean hasExternalFiles(HoodieCommitMetadata commitMetadata) {
    return commitMetadata.getPartitionToWriteStats().values().stream()
        .flatMap(Collection::stream)
        .anyMatch(stat -> isExternallyCreatedFile(stat.getPath())
                          || isExternallyCreatedFile(stat.getFileId()));
  }

  /**
   * Creates a FileSlice for an externally created file using write statistics.
   * This method constructs a FileSlice by extracting file metadata from the provided
   * HoodieWriteStat, including file size, partition path, and file ID. The commit time
   * is parsed from the base file name.
   *
   * @param basePath  the base path of the Hudi table
   * @param writeStat the write statistics containing file metadata (path, size, fileId, partition)
   * @return a FileSlice representing the external file with its base file and empty log file list
   */
  public static FileSlice createExternalFileSlice(StoragePath basePath, HoodieWriteStat writeStat) {
    String fileId = writeStat.getFileId();
    String partitionPath = writeStat.getPartitionPath();
    HoodieFileGroupId fileGroupId = new HoodieFileGroupId(partitionPath, fileId);
    // Create absolute path by combining basePath with relative path from writeStat
    StoragePath absolutePath = new StoragePath(basePath, writeStat.getPath());
    // Create StoragePathInfo from writeStat information
    StoragePathInfo pathInfo = new StoragePathInfo(
        absolutePath,
        writeStat.getFileSizeInBytes(),
        false, // isDirectory
        (short) 0, // blockReplication
        0L, // blockSize
        0L // modificationTime
    );
    HoodieBaseFile baseFile = new HoodieBaseFile(pathInfo);
    return new FileSlice(fileGroupId, baseFile.getCommitTime(), baseFile, Collections.emptyList());
  }

  /**
   * Creates a FileSlice for an externally created file using partition path and file ID.
   * This method constructs a FileSlice by building the absolute file path from the base path,
   * partition path, and file ID. The file size is set to 0 (unknown) and the commit time
   * is parsed from the file ID. This overload is useful when processing replaced files
   * where detailed write statistics are not available.
   *
   * @param basePath     the base path of the Hudi table
   * @param partitionPath the partition path where the file resides (can be empty for non-partitioned tables)
   * @param fileId       the file ID which should contain the commit time
   * @return a FileSlice representing the external file with its base file and empty log file list
   */
  public static FileSlice createExternalFileSlice(StoragePath basePath, String partitionPath, String fileId) {
    HoodieFileGroupId fileGroupId = new HoodieFileGroupId(partitionPath, fileId);

    // Create absolute path - just combine basePath + partitionPath + fileId
    StoragePath absolutePath;
    if (partitionPath.isEmpty()) {
      absolutePath = new StoragePath(basePath, fileId);
    } else {
      absolutePath = new StoragePath(basePath, partitionPath + StoragePath.SEPARATOR + fileId);
    }
    // Create StoragePathInfo for the file
    StoragePathInfo pathInfo = new StoragePathInfo(
        absolutePath,
        0L, // length - unknown
        false, // isDirectory
        (short) 0, // blockReplication
        0L, // blockSize
        0L // modificationTime
    );
    HoodieBaseFile baseFile = new HoodieBaseFile(pathInfo);
    return new FileSlice(fileGroupId, baseFile.getCommitTime(), baseFile, Collections.emptyList());
  }
}
