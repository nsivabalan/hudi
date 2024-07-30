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

package org.apache.hudi.common.table.view;

import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.metadata.HoodieTableMetadata;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

class TestFileSystemViewManager extends HoodieCommonTestHarness {

  @BeforeEach
  public void setup() throws IOException {
    metaClient = HoodieTestUtils.init(tempDir.toAbsolutePath().toString());
    basePath = metaClient.getBasePath();
    refreshFsView();
  }

  @AfterEach
  public void tearDown() {
    cleanMetaClient();
  }

  @Test
  void testSecondaryViewSupplier() throws Exception {
    HoodieCommonConfig config = HoodieCommonConfig.newBuilder().build();
    HoodieEngineContext engineContext = new HoodieLocalEngineContext(new Configuration());
    HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder().enable(true).build();
    try (HoodieTableMetadata metadata = HoodieTableMetadata.create(engineContext, metadataConfig, metaClient.getBasePathV2().toString(), true)) {
      FileSystemViewManager.SecondaryViewCreator inMemorySupplier = new FileSystemViewManager.SecondaryViewCreator(getViewConfig(FileSystemViewStorageType.MEMORY),
          metaClient, config, true, unused -> metadata);
      Assertions.assertTrue(inMemorySupplier.apply(engineContext) instanceof HoodieTableFileSystemView);
      FileSystemViewManager.SecondaryViewCreator spillableSupplier = new FileSystemViewManager.SecondaryViewCreator(getViewConfig(FileSystemViewStorageType.SPILLABLE_DISK),
          metaClient, config, true, unused -> metadata);
      Assertions.assertTrue(spillableSupplier.apply(engineContext) instanceof SpillableMapBasedFileSystemView);
      FileSystemViewManager.SecondaryViewCreator embeddedSupplier = new FileSystemViewManager.SecondaryViewCreator(getViewConfig(FileSystemViewStorageType.EMBEDDED_KV_STORE),
          metaClient, config, true, unused -> metadata);
      Assertions.assertTrue(embeddedSupplier.apply(engineContext) instanceof RocksDbBasedFileSystemView);
      Assertions.assertThrows(IllegalArgumentException.class, () -> new FileSystemViewManager.SecondaryViewCreator(getViewConfig(FileSystemViewStorageType.REMOTE_FIRST),
          metaClient, config, true, unused -> metadata).apply(engineContext));
    }
  }

  private FileSystemViewStorageConfig getViewConfig(FileSystemViewStorageType type) {
    return FileSystemViewStorageConfig.newBuilder()
        .withSecondaryStorageType(type)
        .build();
  }
}