/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.gcp.transaction.lock;

import org.apache.hudi.client.transaction.lock.ConditionalWriteLockConfig;
import org.apache.hudi.client.transaction.lock.ConditionalWriteLockProvider;
import org.apache.hudi.common.config.LockConfiguration;
import org.apache.hudi.common.config.TypedProperties;

import com.google.cloud.NoCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.lang.reflect.Field;
import java.net.URI;
import java.net.URISyntaxException;

import static org.apache.hudi.common.config.HoodieConfig.BASE_PATH_KEY;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class TestGCSConditionalWriteLockProvider
    extends AbstractLockProviderTestBase {

  private static final DockerImageName FAKE_GCS_IMAGE =
      DockerImageName.parse("fsouza/fake-gcs-server:latest");

  private static GenericContainer<?> GCS_CONTAINER;
  private static String endpoint;
  private static String testBucket = "test-bucket";
  protected static Storage storage;

  @BeforeAll
  static void initContainer() {
    // Start the container
    GCS_CONTAINER = new GenericContainer<>(FAKE_GCS_IMAGE)
        .withExposedPorts(4443)
        .withCommand("-scheme http");

    GCS_CONTAINER.start();

    Integer mappedPort = GCS_CONTAINER.getMappedPort(4443);
    endpoint = String.format("http://%s:%d", GCS_CONTAINER.getHost(), mappedPort);

    storage = StorageOptions.newBuilder()
        .setProjectId("test-project")
        .setCredentials(NoCredentials.getInstance())
        .setHost(endpoint)
        .build()
        .getService();

    storage.create(Bucket.newBuilder(testBucket).build());
    Bucket retrievedBucket = storage.get(testBucket);
    assertNotNull(retrievedBucket, "Bucket " + testBucket + " should exist but does not.");
  }

  @Override
  protected ConditionalWriteLockProvider createLockProvider() {
    LockConfiguration lockConf = new LockConfiguration(providerProperties);
    Configuration conf = new Configuration();
    try (MockedStatic<StorageOptions> storageOptionsMock = mockStatic(StorageOptions.class)) {
      StorageOptions.Builder builderMock = mock(StorageOptions.Builder.class);
      StorageOptions storageOptionsInstanceMock = mock(StorageOptions.class);
      storageOptionsMock.when(StorageOptions::newBuilder).thenReturn(builderMock);
      when(builderMock.build()).thenReturn(storageOptionsInstanceMock);
      when(storageOptionsInstanceMock.getService()).thenReturn(storage);
      return new ConditionalWriteLockProvider(
          lockConf,
          conf);
    }
  }

  @BeforeEach
  void setupLockProvider() {
    providerProperties.put(ConditionalWriteLockConfig.LOCK_INTERNAL_STORAGE_LOCATION.key(), "gs://" + testBucket + "/locks");
    providerProperties.put(BASE_PATH_KEY, "gs://bucket/lake/db/tbl-default");
    recreateLockProvider();
  }

  @AfterAll
  static void stopContainer() {
    if (GCS_CONTAINER != null) {
      GCS_CONTAINER.stop();
    }
  }

  @Test
  void testValidDefaultConstructor() {
    TypedProperties props = new TypedProperties();
    props.put(ConditionalWriteLockConfig.LOCK_INTERNAL_STORAGE_LOCATION.key(), "gs://test-bucket/locks");
    props.put(ConditionalWriteLockConfig.BASE_PATH_KEY, "gs://bucket/lake/db/tbl-default");
    props.put(ConditionalWriteLockConfig.LOCK_VALIDITY_TIMEOUT_MS.key(), "5000");
    props.put(ConditionalWriteLockConfig.HEARTBEAT_POLL_MS.key(), "1000");

    LockConfiguration lockConf = new LockConfiguration(props);
    Configuration conf = new Configuration();

    ConditionalWriteLockProvider provider = new ConditionalWriteLockProvider(lockConf, conf);
    assertNull(provider.getLock());
    provider.close();
  }

  @Test
  void testValidDefaultConstructorWithWeirdBasePath() {
    TypedProperties props = new TypedProperties();
    props.put(ConditionalWriteLockConfig.LOCK_INTERNAL_STORAGE_LOCATION.key(), "gs://test-bucket/locks");
    props.put(ConditionalWriteLockConfig.BASE_PATH_KEY, "//中文/路径//测试/emoji-u83DuDE0E-text//\\\\invalid*chars%$#end\n");
    props.put(ConditionalWriteLockConfig.LOCK_VALIDITY_TIMEOUT_MS.key(), "5000");
    props.put(ConditionalWriteLockConfig.HEARTBEAT_POLL_MS.key(), "1000");

    LockConfiguration lockConf = new LockConfiguration(props);
    Configuration conf = new Configuration();

    ConditionalWriteLockProvider provider = new ConditionalWriteLockProvider(lockConf, conf);
    try {
      Field field = provider.getClass().getDeclaredField("lockFilePath");
      field.setAccessible(true);
      String lockFilePath = (String) field.get(provider);
      new URI(lockFilePath);
    } catch (URISyntaxException | NoSuchFieldException | IllegalAccessException e) {
      fail("Should not throw exception!");
    }
  }

  @Test
  void testGcsPreconditions() {
    // Simple test to validate GCS preconditions with generation numbers.
    Blob b1 = storage.create(BlobInfo.newBuilder(
            BlobId.of("test-bucket", "myblob")).build(),
        new byte[] { 0xf },
        Storage.BlobTargetOption.generationMatch(0));
    Blob b2 = storage.create(BlobInfo.newBuilder(
            BlobId.of("test-bucket", "myblob")).build(),
        new byte[] { 0xd },
        Storage.BlobTargetOption.generationMatch(b1.getGeneration()));
    assertNotEquals(b1.getGeneration(), b2.getGeneration());
  }
}