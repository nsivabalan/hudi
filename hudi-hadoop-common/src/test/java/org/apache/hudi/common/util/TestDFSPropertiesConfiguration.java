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

package org.apache.hudi.common.util;

import org.apache.hudi.common.config.DFSPropertiesConfiguration;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.testutils.minicluster.HdfsTestService;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.storage.StoragePath;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.Rule;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests basic functionality of {@link DFSPropertiesConfiguration}.
 */
public class TestDFSPropertiesConfiguration {

  private static String dfsBasePath;
  private static HdfsTestService hdfsTestService;
  private static MiniDFSCluster dfsCluster;
  private static DistributedFileSystem dfs;

  @Rule
  public final EnvironmentVariables environmentVariables
      = new EnvironmentVariables();

  @BeforeAll
  public static void initClass() throws Exception {
    if (HoodieTestUtils.shouldUseExternalHdfs()) {
      dfs = HoodieTestUtils.useExternalHdfs();
    } else {
      hdfsTestService = new HdfsTestService();
      dfsCluster = hdfsTestService.start(true);
      dfs = dfsCluster.getFileSystem();
    }

    // Create a temp folder as the base path
    dfsBasePath = dfs.getWorkingDirectory().toString();
    dfs.mkdirs(new Path(dfsBasePath));

    // create some files.
    Path filePath = new Path(dfsBasePath + "/t1.props");
    writePropertiesFile(filePath, new String[] {"", "#comment", "abc", // to be ignored
        "int.prop=123", "double.prop=113.4", "string.prop=str", "boolean.prop=true", "long.prop=1354354354"});

    filePath = new Path(dfsBasePath + "/t2.props");
    writePropertiesFile(filePath, new String[] {"string.prop=ignored", "include=" + dfsBasePath + "/t1.props"});

    filePath = new Path(dfsBasePath + "/t3.props");
    writePropertiesFile(filePath,
        new String[] {"double.prop=838.3", "include=" + "t2.props", "double.prop=243.4", "string.prop=t3.value"});

    filePath = new Path(dfsBasePath + "/t4.props");
    writePropertiesFile(filePath, new String[] {"double.prop=838.3", "include = t4.props"});
  }

  @AfterAll
  public static void cleanupClass() {
    if (hdfsTestService != null) {
      hdfsTestService.stop();
    }
  }

  @AfterEach
  public void cleanupGlobalConfig() {
    DFSPropertiesConfiguration.clearGlobalProps();
  }

  private static void writePropertiesFile(Path path, String[] lines) throws IOException {
    PrintStream out = new PrintStream(dfs.create(path, true));
    for (String line : lines) {
      out.println(line);
    }
    out.flush();
    out.close();
  }

  @Test
  public void testParsing() {
    DFSPropertiesConfiguration cfg = new DFSPropertiesConfiguration(
        dfs.getConf(), new StoragePath(dfsBasePath + "/t1.props"));
    TypedProperties props = cfg.getProps();
    assertEquals(5, props.size());
    assertThrows(IllegalArgumentException.class, () -> {
      props.getString("invalid.key");
    }, "Should error out here.");

    assertEquals(123, props.getInteger("int.prop"));
    assertEquals(113.4, props.getDouble("double.prop"), 0.001);
    assertTrue(props.getBoolean("boolean.prop"));
    assertEquals("str", props.getString("string.prop"));
    assertEquals(1354354354, props.getLong("long.prop"));

    assertEquals(123, props.getInteger("int.prop", 456));
    assertEquals(113.4, props.getDouble("double.prop", 223.4), 0.001);
    assertTrue(props.getBoolean("boolean.prop", false));
    assertEquals("str", props.getString("string.prop", "default"));
    assertEquals(1354354354, props.getLong("long.prop", 8578494434L));

    assertEquals(456, props.getInteger("bad.int.prop", 456));
    assertEquals(223.4, props.getDouble("bad.double.prop", 223.4), 0.001);
    assertFalse(props.getBoolean("bad.boolean.prop", false));
    assertEquals("default", props.getString("bad.string.prop", "default"));
    assertEquals(8578494434L, props.getLong("bad.long.prop", 8578494434L));
  }

  @Test
  public void testIncludes() {
    DFSPropertiesConfiguration cfg = new DFSPropertiesConfiguration(
        dfs.getConf(), new StoragePath(dfsBasePath + "/t3.props"));
    TypedProperties props = cfg.getProps();

    assertEquals(123, props.getInteger("int.prop"));
    assertEquals(243.4, props.getDouble("double.prop"), 0.001);
    assertTrue(props.getBoolean("boolean.prop"));
    assertEquals("t3.value", props.getString("string.prop"));
    assertEquals(1354354354, props.getLong("long.prop"));
    assertThrows(IllegalStateException.class, () -> {
      cfg.addPropsFromFile(new StoragePath(dfsBasePath + "/t4.props"));
    }, "Should error out on a self-included file.");
  }

  @Test
  public void testLocalFileSystemLoading() throws IOException {
    DFSPropertiesConfiguration cfg = new DFSPropertiesConfiguration(
        dfs.getConf(), new StoragePath(dfsBasePath + "/t1.props"));

    cfg.addPropsFromFile(
        new StoragePath(
            String.format(
                "file:%s",
                getClass().getClassLoader()
                    .getResource("props/testdfs.properties")
                    .getPath()
            )
        ));

    TypedProperties props = cfg.getProps();

    assertEquals(123, props.getInteger("int.prop"));
    assertEquals(113.4, props.getDouble("double.prop"), 0.001);
    assertTrue(props.getBoolean("boolean.prop"));
    assertEquals("str", props.getString("string.prop"));
    assertEquals(1354354354, props.getLong("long.prop"));
    assertEquals(123, props.getInteger("some.random.prop"));
  }

  @Test
  public void testNoGlobalConfFileConfigured() {
    environmentVariables.clear(DFSPropertiesConfiguration.CONF_FILE_DIR_ENV_NAME);
    DFSPropertiesConfiguration.refreshGlobalProps();
    try {
      if (!HoodieTestUtils.getStorage(DFSPropertiesConfiguration.DEFAULT_PATH)
          .exists(DFSPropertiesConfiguration.DEFAULT_PATH)) {
        assertEquals(0, DFSPropertiesConfiguration.getGlobalProps().size());
      }
    } catch (IOException e) {
      throw new HoodieIOException("Cannot check if the default config file exist: " + DFSPropertiesConfiguration.DEFAULT_PATH);
    }
  }

  @Test
  public void testLoadGlobalConfFile() {
    // set HUDI_CONF_DIR
    String testPropsFilePath = new File("src/test/resources/external-config").getAbsolutePath();
    environmentVariables.set(DFSPropertiesConfiguration.CONF_FILE_DIR_ENV_NAME, testPropsFilePath);

    DFSPropertiesConfiguration.refreshGlobalProps();
    assertEquals(5, DFSPropertiesConfiguration.getGlobalProps().size());
    assertEquals("jdbc:hive2://localhost:10000", DFSPropertiesConfiguration.getGlobalProps().get("hoodie.datasource.hive_sync.jdbcurl"));
    assertEquals("true", DFSPropertiesConfiguration.getGlobalProps().get("hoodie.datasource.hive_sync.use_jdbc"));
    assertEquals("false", DFSPropertiesConfiguration.getGlobalProps().get("hoodie.datasource.hive_sync.support_timestamp"));
    assertEquals("BLOOM", DFSPropertiesConfiguration.getGlobalProps().get("hoodie.index.type"));
    assertEquals("true", DFSPropertiesConfiguration.getGlobalProps().get("hoodie.metadata.enable"));
  }

  @Test
  public void testDefaultConstructorHandlesIncludes() {
    // Use default ctor (hadoopConfig should be non-null internally)
    DFSPropertiesConfiguration cfg = new DFSPropertiesConfiguration();

    // Should load t3.props (which includes t2.props which includes t1.props) without NPE
    cfg.addPropsFromFile(new StoragePath(dfsBasePath + "/t3.props"));
    TypedProperties props = cfg.getProps();

    // Values from t1, t2 and t3 should be resolved in order
    assertEquals(123, props.getInteger("int.prop"));
    assertEquals(243.4, props.getDouble("double.prop"), 0.001);
    assertTrue(props.getBoolean("boolean.prop"));
    assertEquals("t3.value", props.getString("string.prop"));
    assertEquals(1354354354L, props.getLong("long.prop"));

    // And a self include still triggers the loop detection
    assertThrows(IllegalStateException.class, () -> {
      cfg.addPropsFromFile(new StoragePath(dfsBasePath + "/t4.props"));
    });
  }

  @Test
  void testReflectionUtilsLoadConfigValueWithTrueSetting() throws Exception {
    environmentVariables.set("hoodie_reflection_usethreadcontext", "true");
    // Reset the cached value in ReflectionUtils
    Field useThreadContextField = ReflectionUtils.class.getDeclaredField("useThreadContextClassLoader");
    useThreadContextField.setAccessible(true);
    useThreadContextField.set(null, null);
    // Clear the class cache
    Field cacheField = ReflectionUtils.class.getDeclaredField("CLAZZ_CACHE");
    cacheField.setAccessible(true);
    @SuppressWarnings("unchecked")
    java.util.Map<String, Class<?>> cache = (java.util.Map<String, Class<?>>) cacheField.get(null);
    cache.clear();
    // Test that ReflectionUtils can load a class using thread context class loader
    Class<?> clazz = ReflectionUtils.getClass("java.lang.String");
    assertNotNull(clazz);
    assertEquals(String.class, clazz);
    // Verify that the thread context class loader setting is working
    assertTrue(ReflectionUtils.shouldUseThreadContextClassLoader());
  }

  @Test
  void testReflectionUtilsLoadConfigValueWithFalseSetting() throws Exception {
    environmentVariables.set("hoodie_reflection_usethreadcontext", "false");

    // Reset the cached value in ReflectionUtils
    Field useThreadContextField = ReflectionUtils.class.getDeclaredField("useThreadContextClassLoader");
    useThreadContextField.setAccessible(true);
    useThreadContextField.set(null, null);
    // Clear the class cache
    Field cacheField = ReflectionUtils.class.getDeclaredField("CLAZZ_CACHE");
    cacheField.setAccessible(true);
    @SuppressWarnings("unchecked")
    java.util.Map<String, Class<?>> cache = (java.util.Map<String, Class<?>>) cacheField.get(null);
    cache.clear();
    // Test that ReflectionUtils can still load a class using system class loader
    Class<?> clazz = ReflectionUtils.getClass("java.lang.String");
    assertNotNull(clazz);
    assertEquals(String.class, clazz);
    // Verify that the thread context class loader setting is working
    assertFalse(ReflectionUtils.shouldUseThreadContextClassLoader());
  }

  @Test
  void testReflectionUtilsLoadConfigValueWithDefaultSetting() throws Exception {
    environmentVariables.clear("hoodie_reflection_usethreadcontext");
    // Reset the cached value in ReflectionUtils
    Field useThreadContextField = ReflectionUtils.class.getDeclaredField("useThreadContextClassLoader");
    useThreadContextField.setAccessible(true);
    useThreadContextField.set(null, null);
    // Clear the class cache
    Field cacheField = ReflectionUtils.class.getDeclaredField("CLAZZ_CACHE");
    cacheField.setAccessible(true);
    @SuppressWarnings("unchecked")
    java.util.Map<String, Class<?>> cache = (java.util.Map<String, Class<?>>) cacheField.get(null);
    cache.clear();
    // Test that ReflectionUtils can load a class using system class loader
    Class<?> clazz = ReflectionUtils.getClass("java.lang.String");
    assertNotNull(clazz);
    assertEquals(String.class, clazz);
    // Verify that the thread context class loader setting defaults to false
    assertFalse(ReflectionUtils.shouldUseThreadContextClassLoader());
  }

  @Test
  void testReflectionUtilsLoadConfigValueWithInvalidSetting() throws Exception {
    environmentVariables.set("hoodie_reflection_usethreadcontext", "invalid");
    // Reset the cached value in ReflectionUtils
    Field useThreadContextField = ReflectionUtils.class.getDeclaredField("useThreadContextClassLoader");
    useThreadContextField.setAccessible(true);
    useThreadContextField.set(null, null);
    // Clear the class cache
    Field cacheField = ReflectionUtils.class.getDeclaredField("CLAZZ_CACHE");
    cacheField.setAccessible(true);
    @SuppressWarnings("unchecked")
    java.util.Map<String, Class<?>> cache = (java.util.Map<String, Class<?>>) cacheField.get(null);
    cache.clear();
    // Test that ReflectionUtils can still load a class using system class loader
    Class<?> clazz = ReflectionUtils.getClass("java.lang.String");
    assertNotNull(clazz);
    assertEquals(String.class, clazz);
    // Verify that the thread context class loader setting defaults to false for invalid values
    assertFalse(ReflectionUtils.shouldUseThreadContextClassLoader());
  }

  @Test
  void testReflectionUtilsLoadConfigValueCaching() throws Exception {
    environmentVariables.set("hoodie_reflection_usethreadcontext", "true");
    // Reset the cached value in ReflectionUtils
    Field useThreadContextField = ReflectionUtils.class.getDeclaredField("useThreadContextClassLoader");
    useThreadContextField.setAccessible(true);
    useThreadContextField.set(null, null);
    // Clear the class cache
    Field cacheField = ReflectionUtils.class.getDeclaredField("CLAZZ_CACHE");
    cacheField.setAccessible(true);
    @SuppressWarnings("unchecked")
    java.util.Map<String, Class<?>> cache = (java.util.Map<String, Class<?>>) cacheField.get(null);
    cache.clear();
    // First call should load the config value
    boolean firstResult = ReflectionUtils.shouldUseThreadContextClassLoader();
    assertTrue(firstResult);
    // Change the config value
    environmentVariables.set("hoodie_reflection_usethreadcontext", "false");
    // Second call should return cached value (not the new value)
    boolean secondResult = ReflectionUtils.shouldUseThreadContextClassLoader();
    assertTrue(secondResult);
    // Reset cache and test again
    useThreadContextField.set(null, null);
    boolean thirdResult = ReflectionUtils.shouldUseThreadContextClassLoader();
    assertFalse(thirdResult);
  }

  @Test
  public void testLazyInitializationWithFailureAndRetry() {
    DFSPropertiesConfiguration.clearGlobalProps();

    environmentVariables.clear(DFSPropertiesConfiguration.CONF_FILE_DIR_ENV_NAME);
    TypedProperties props1 = DFSPropertiesConfiguration.getGlobalProps();
    assertTrue(props1 != null);

    String testPropsFilePath = new File("src/test/resources/external-config").getAbsolutePath();
    environmentVariables.set(DFSPropertiesConfiguration.CONF_FILE_DIR_ENV_NAME, testPropsFilePath);

    DFSPropertiesConfiguration.clearGlobalProps();
    DFSPropertiesConfiguration.refreshGlobalProps();

    TypedProperties props2 = DFSPropertiesConfiguration.getGlobalProps();
    assertEquals(5, props2.size());
    assertEquals("jdbc:hive2://localhost:10000", props2.get("hoodie.datasource.hive_sync.jdbcurl"));

    DFSPropertiesConfiguration.clearGlobalProps();
    TypedProperties props3 = DFSPropertiesConfiguration.getGlobalProps();
    assertEquals(0, props3.size());
  }

  @Test
  public void testClassInitializationNeverThrows() {
    DFSPropertiesConfiguration.clearGlobalProps();
    environmentVariables.set(DFSPropertiesConfiguration.CONF_FILE_DIR_ENV_NAME, "/this/path/does/not/exist/at/all");

    try {
      DFSPropertiesConfiguration.getGlobalProps();
    } catch (HoodieIOException e) {
      // Expected for non-existent path
    }

    // Verify class is not poisoned - instance methods still work
    DFSPropertiesConfiguration cfg = new DFSPropertiesConfiguration();
    cfg.addPropsFromFile(new StoragePath(dfsBasePath + "/t1.props"));
    TypedProperties props = cfg.getProps();
    assertEquals(5, props.size());
  }

  @Test
  public void testAddToGlobalProps() {
    DFSPropertiesConfiguration.clearGlobalProps();

    String testPropsFilePath = new File("src/test/resources/external-config").getAbsolutePath();
    environmentVariables.set(DFSPropertiesConfiguration.CONF_FILE_DIR_ENV_NAME, testPropsFilePath);
    DFSPropertiesConfiguration.refreshGlobalProps();

    TypedProperties result = DFSPropertiesConfiguration.addToGlobalProps("test.key1", "test.value1");
    assertEquals("test.value1", result.get("test.key1"));
    assertEquals(6, result.size());

    TypedProperties globals = DFSPropertiesConfiguration.getGlobalProps();
    assertEquals("test.value1", globals.get("test.key1"));

    DFSPropertiesConfiguration.addToGlobalProps("test.key2", "test.value2");
    globals = DFSPropertiesConfiguration.getGlobalProps();
    assertEquals("test.value1", globals.get("test.key1"));
    assertEquals("test.value2", globals.get("test.key2"));
    assertEquals(7, globals.size());

    DFSPropertiesConfiguration.clearGlobalProps();
    environmentVariables.clear(DFSPropertiesConfiguration.CONF_FILE_DIR_ENV_NAME);

    result = DFSPropertiesConfiguration.addToGlobalProps("test.key3", "test.value3");
    assertEquals("test.value3", result.get("test.key3"));
    assertEquals(1, result.size());
  }

  @Test
  public void testIncludeNonExistentFile() throws IOException {
    // Create a properties file that includes a non-existent file
    Path filePath = new Path(dfsBasePath + "/t5.props");
    writePropertiesFile(filePath, new String[] {
        "existing.prop=value1",
        "include=" + dfsBasePath + "/non-existent-file.props",
        "another.prop=value2"
    });

    // Should not throw an exception, but log a warning and continue
    DFSPropertiesConfiguration cfg = new DFSPropertiesConfiguration(dfs.getConf(), HadoopFSUtils.convertToStoragePath(filePath));
    TypedProperties props = cfg.getProps();

    // Properties before and after the non-existent include should still be loaded
    assertEquals(2, props.size());
    assertEquals("value1", props.getString("existing.prop"));
    assertEquals("value2", props.getString("another.prop"));
  }

  @Test
  public void testIncludeNonExistentRelativeFile() throws IOException {
    // Create a properties file that includes a non-existent relative file
    Path filePath = new Path(dfsBasePath + "/t6.props");
    writePropertiesFile(filePath, new String[] {
        "prop1=val1",
        "include=non-existent-relative.props",
        "prop2=val2"
    });

    // Should not throw an exception for non-existent relative includes
    DFSPropertiesConfiguration cfg = new DFSPropertiesConfiguration(dfs.getConf(), HadoopFSUtils.convertToStoragePath(filePath));
    TypedProperties props = cfg.getProps();

    // Properties before and after the non-existent include should still be loaded
    assertEquals(2, props.size());
    assertEquals("val1", props.getString("prop1"));
    assertEquals("val2", props.getString("prop2"));
  }

  @Test
  public void testMixedExistentAndNonExistentIncludes() throws IOException {
    // Create a properties file with both existent and non-existent includes
    Path filePath = new Path(dfsBasePath + "/t7.props");
    writePropertiesFile(filePath, new String[] {
        "base.prop=base_value",
        "include=" + dfsBasePath + "/non-existent-1.props",
        "include=" + dfsBasePath + "/t1.props",  // This exists
        "include=" + dfsBasePath + "/non-existent-2.props",
        "override.prop=override_value"
    });

    // Should load successfully, ignoring non-existent files
    DFSPropertiesConfiguration cfg = new DFSPropertiesConfiguration(dfs.getConf(), HadoopFSUtils.convertToStoragePath(filePath));
    TypedProperties props = cfg.getProps();

    // Should have properties from t1.props and the main file
    assertEquals("base_value", props.getString("base.prop"));
    assertEquals("override_value", props.getString("override.prop"));
    assertEquals(123, props.getInteger("int.prop"));  // From t1.props
    assertEquals("str", props.getString("string.prop"));  // From t1.props
    assertTrue(props.getBoolean("boolean.prop"));  // From t1.props
  }
}
