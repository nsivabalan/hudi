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

package org.apache.hudi.utilities.benchmarking;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.io.compress.CompressionCodec;
import org.apache.hudi.io.hfile.HFileContext;
import org.apache.hudi.io.hfile.HFileWriterImpl;
import org.apache.hudi.io.storage.HFileReaderFactory;
import org.apache.hudi.io.storage.HoodieNativeAvroHFileReader;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * Benchmark tool to measure HFile prefix read performance.
 *
 * Creates an HFile with 1M entries where each prefix matches 10,000 entries.
 * Tests prefix read performance for 1, 10, 30, and 50 keys under three scenarios:
 * a) Normal prefix read (without downloading entire file upfront)
 * b) Download entire file upfront, then do prefix read
 * c) Download entire file upfront, then iteratively read all entries and filter
 */
public class HFilePrefixReadBenchmark {

  private static final Logger LOG = LoggerFactory.getLogger(HFilePrefixReadBenchmark.class);

  // Constants for HFile generation
  private static final int TOTAL_ENTRIES = 1_000_000;
  private static final int ENTRIES_PER_PREFIX = 10_000;
  private static final int NUM_PREFIXES = TOTAL_ENTRIES / ENTRIES_PER_PREFIX; // 100 prefixes
  private static final String BASE_PREFIX = "aaaaaaaa";

  // Value payload size
  private static final int VALUE_SIZE = 100;

  private static final Schema SCHEMA = SchemaBuilder.record("TestRecord")
      .fields()
      .name("key").type().stringType().noDefault()
      .name("value").type().stringType().noDefault()
      .endRecord();

  public static class Config implements Serializable {
    @Parameter(names = {"--output-dir", "-o"}, description = "Output directory for HFile", required = true)
    public String outputDir = null;

    @Parameter(names = {"--num-keys", "-n"}, description = "Number of prefix keys to test (comma-separated, e.g., '1,10,30,50')")
    public String numKeys = "1,10,30,50";

    @Parameter(names = {"--help", "-h"}, help = true)
    public Boolean help = false;

    @Override
    public String toString() {
      return "HFilePrefixReadBenchmark {\n"
          + "   --output-dir " + outputDir + ",\n"
          + "   --num-keys " + numKeys + "\n"
          + "}";
    }
  }

  private final Config cfg;
  private final StoragePath hfilePath;
  private final HoodieStorage storage;

  public HFilePrefixReadBenchmark(SparkSession spark, Config cfg) throws IOException {
    this.cfg = cfg;
    StoragePath outputDirPath = new StoragePath(cfg.outputDir);
    this.storage = new HoodieHadoopStorage(outputDirPath, new HadoopStorageConfiguration(spark.sparkContext().hadoopConfiguration()));

    // Generate filename with first and last prefix
    String firstPrefix = generatePrefix(0);
    String lastPrefix = generatePrefix(NUM_PREFIXES - 1);
    String filename = String.format("hfile_1M_entries_prefix_%s_to_%s.hfile", firstPrefix, lastPrefix);
    this.hfilePath = new StoragePath(outputDirPath, filename);

    LOG.info("HFile will be created at: {}", hfilePath);
    LOG.info("First prefix: {}, Last prefix: {}", firstPrefix, lastPrefix);
  }

  public static void main(String[] args) {
    final Config cfg = new Config();
    JCommander cmd = new JCommander(cfg, null, args);

    if (cfg.help || args.length == 0) {
      cmd.usage();
      System.exit(1);
    }

    final LocalDateTime now = LocalDateTime.now();
    final String currentHour = now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd-HH"));
    String jobName = "hfile-read-benchmark";
    String sparkAppName = jobName + "-" + currentHour;
    SparkSession spark = SparkSession.builder()
        .appName(sparkAppName)
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate();

    try {
      HFilePrefixReadBenchmark benchmark = new HFilePrefixReadBenchmark(spark, cfg);
      benchmark.run();
    } catch (Exception e) {
      LOG.error("Failed to run benchmark: " + cfg, e);
      throw new RuntimeException("Failed to run benchmark", e);
    }
  }

  public void run() throws Exception {
    LOG.info("Starting HFile Prefix Read Benchmark");
    LOG.info("Config: {}", cfg);

    // Step 1: Create HFile with 1M entries
    LOG.info("Step 1: Creating HFile with {} entries ({} prefixes, {} entries per prefix)",
        TOTAL_ENTRIES, NUM_PREFIXES, ENTRIES_PER_PREFIX);
    createHFile();
    LOG.info("HFile created successfully at: {}", hfilePath);

    // Parse num keys to test
    List<Integer> numKeysList = parseNumKeys(cfg.numKeys);
    LOG.info("Testing with key counts: {}", numKeysList);

    // Step 2: Run benchmarks for each key count
    for (int numKeys : numKeysList) {
      LOG.info("\n========================================");
      LOG.info("Testing with {} prefix keys", numKeys);
      LOG.info("========================================");

      List<String> prefixKeys = selectPrefixKeys(numKeys);
      LOG.info("Selected prefixes: {}", prefixKeys);

      runBenchmarks(prefixKeys);
    }

    LOG.info("\nBenchmark completed successfully!");
  }

  /**
   * Creates an HFile with 1M entries using Hudi's HFileWriterImpl.
   * Key format: BASE_PREFIX + 2-char prefix + 4-digit suffix
   * Example: aaaaaaaaaaa0000, aaaaaaaaaaa0001, ..., aaaaaaaadr9999
   */
  private void createHFile() throws IOException {
    long startTime = System.currentTimeMillis();

    // Create Hudi HFileContext
    HFileContext context = HFileContext.builder()
        .blockSize(64 * 1024) // 64KB blocks
        .compressionCodec(CompressionCodec.GZIP)
        .build();

    // Create output stream for the HFile
    OutputStream outputStream = storage.create(hfilePath);

    // Use Hudi's HFileWriterImpl
    HFileWriterImpl writer = new HFileWriterImpl(context, outputStream);

    // Write schema metadata
    writer.appendFileInfo("schema", SCHEMA.toString().getBytes(StandardCharsets.UTF_8));
    HoodieSchema hoodieSchema = HoodieSchema.fromAvroSchema(SCHEMA);

    String minRecordKey = null;
    String maxRecordKey = null;
    int entriesWritten = 0;
    for (int prefixIdx = 0; prefixIdx < NUM_PREFIXES; prefixIdx++) {
      String prefix = generatePrefix(prefixIdx);

      for (int suffix = 0; suffix < ENTRIES_PER_PREFIX; suffix++) {
        String key = generateKey(prefix, suffix);
        String value = generateValue(key);

        GenericRecord record = new GenericData.Record(SCHEMA);
        record.put("key", key);
        record.put("value", value);

        // Track min and max record keys
        if (minRecordKey == null) {
          minRecordKey = key;
        }
        maxRecordKey = key;

        // Use HFileWriterImpl's append method which takes String key and byte[] value
        //Option<HoodieSchemaField> keyFieldOpt = hoodieSchema.getField("key");
        //final byte[] recordBytes = serializeRecord(record, hoodieSchema, keyFieldOpt);
        writer.append(key, HoodieAvroUtils.avroToBytes(record));
        entriesWritten++;

        if (entriesWritten % 100000 == 0) {
          LOG.info("Written {} entries...", entriesWritten);
        }
      }
    }

    // Write min and max record key metadata before closing
    if (minRecordKey != null) {
      writer.appendFileInfo("minRecordKey", minRecordKey.getBytes(StandardCharsets.UTF_8));
      writer.appendFileInfo("maxRecordKey", maxRecordKey.getBytes(StandardCharsets.UTF_8));
    }

    writer.close();

    long duration = System.currentTimeMillis() - startTime;
    long fileSizeBytes = storage.getPathInfo(hfilePath).getLength();
    LOG.info("Created HFile with {} entries in {} ms", entriesWritten, duration);
    LOG.info("File size: {} MB", fileSizeBytes / (1024 * 1024));
  }

  /**
   * Generates a 2-character prefix for the given index.
   * Uses base-26 (a-z) to generate prefixes: aa, ab, ac, ..., zz
   */
  private String generatePrefix(int index) {
    char first = (char) ('a' + (index / 26));
    char second = (char) ('a' + (index % 26));
    return "" + first + second;
  }

  /**
   * Generates a key: BASE_PREFIX + prefix + 4-digit suffix
   */
  private String generateKey(String prefix, int suffix) {
    return BASE_PREFIX + prefix + String.format("%04d", suffix);
  }

  /**
   * Generates a value string of fixed size
   */
  private String generateValue(String key) {
    StringBuilder sb = new StringBuilder(VALUE_SIZE);
    sb.append("value_for_").append(key);
    while (sb.length() < VALUE_SIZE) {
      sb.append("_padding");
    }
    return sb.substring(0, VALUE_SIZE);
  }

  /**
   * Parses the comma-separated num keys string
   */
  private List<Integer> parseNumKeys(String numKeysStr) {
    List<Integer> result = new ArrayList<>();
    for (String s : numKeysStr.split(",")) {
      result.add(Integer.parseInt(s.trim()));
    }
    return result;
  }

  /**
   * Selects prefix keys for testing.
   * Distributes them evenly across the prefix range.
   */
  private List<String> selectPrefixKeys(int count) {
    List<String> prefixes = new ArrayList<>();
    int step = NUM_PREFIXES / count;
    if (step == 0) {
      step = 1;
    }

    for (int i = 0; i < count && i * step < NUM_PREFIXES; i++) {
      prefixes.add(BASE_PREFIX + generatePrefix(i * step));
    }

    return prefixes;
  }

  /**
   * Runs all three benchmark scenarios
   */
  private void runBenchmarks(List<String> prefixKeys) throws Exception {
    LOG.info("\n--- Scenario A: Prefix read without downloading entire file ---");
    runScenarioA(prefixKeys);

    LOG.info("\n--- Scenario B: Download entire file, then prefix read ---");
    runScenarioB(prefixKeys);

    LOG.info("\n--- Scenario C: Download entire file, then iterative read with filter ---");
    runScenarioC(prefixKeys);
  }

  /**
   * Scenario A: Normal prefix read without downloading entire file upfront
   */
  private void runScenarioA(List<String> prefixKeys) throws Exception {
    long startTime = System.currentTimeMillis();

    // Configure to NOT download entire file (small cache size)
    TypedProperties props = new TypedProperties();
    props.setProperty("hoodie.metadata.file.cache.max.size.mb", "0");

    HFileReaderFactory readerFactory = HFileReaderFactory.builder()
        .withStorage(storage)
        .withPath(hfilePath)
        .withProps(props)
        .build();

    HoodieSchema hoodieSchema = HoodieSchema.fromAvroSchema(SCHEMA);
    HoodieNativeAvroHFileReader reader = HoodieNativeAvroHFileReader.builder()
        .readerFactory(readerFactory)
        .schema(Option.of(hoodieSchema))
        .path(hfilePath)
        .build();

    int totalRecordsRead = 0;
    try (ClosableIterator<IndexedRecord> iterator =
            reader.getIndexedRecordsByKeyPrefixIterator(prefixKeys, hoodieSchema)) {
      while (iterator.hasNext()) {
        IndexedRecord record = iterator.next();
        totalRecordsRead++;
      }
    }

    reader.close();

    long duration = System.currentTimeMillis() - startTime;
    int expectedRecords = prefixKeys.size() * ENTRIES_PER_PREFIX;

    LOG.info("Scenario A Results:");
    LOG.info("  Duration: {} ms", duration);
    LOG.info("  Records read: {}", totalRecordsRead);
    LOG.info("  Expected records: {}", expectedRecords);
    LOG.info("  Match: {}", totalRecordsRead == expectedRecords);
  }

  /**
   * Scenario B: Download entire file upfront, then do prefix read
   */
  private void runScenarioB(List<String> prefixKeys) throws Exception {
    long startTime = System.currentTimeMillis();

    // Configure to download entire file upfront (large cache size)
    TypedProperties props = new TypedProperties();
    long fileSizeMB = storage.getPathInfo(hfilePath).getLength() / (1024 * 1024) + 1;
    props.setProperty("hoodie.metadata.file.cache.max.size.mb", String.valueOf(fileSizeMB * 2));

    HFileReaderFactory readerFactory = HFileReaderFactory.builder()
        .withStorage(storage)
        .withPath(hfilePath)
        .withProps(props)
        .build();

    HoodieNativeAvroHFileReader reader = HoodieNativeAvroHFileReader.builder()
        .readerFactory(readerFactory)
        .path(hfilePath)
        .build();

    HoodieSchema hoodieSchema = HoodieSchema.fromAvroSchema(SCHEMA);

    int totalRecordsRead = 0;
    try (ClosableIterator<IndexedRecord> iterator =
            reader.getIndexedRecordsByKeyPrefixIterator(prefixKeys, hoodieSchema)) {
      while (iterator.hasNext()) {
        IndexedRecord record = iterator.next();
        totalRecordsRead++;
      }
    }

    reader.close();

    long duration = System.currentTimeMillis() - startTime;
    int expectedRecords = prefixKeys.size() * ENTRIES_PER_PREFIX;

    LOG.info("Scenario B Results:");
    LOG.info("  Duration: {} ms", duration);
    LOG.info("  Records read: {}", totalRecordsRead);
    LOG.info("  Expected records: {}", expectedRecords);
    LOG.info("  Match: {}", totalRecordsRead == expectedRecords);
  }

  /**
   * Scenario C: Download entire file upfront, then iteratively read all entries and filter
   */
  private void runScenarioC(List<String> prefixKeys) throws Exception {
    long startTime = System.currentTimeMillis();

    // Configure to download entire file upfront (large cache size)
    TypedProperties props = new TypedProperties();
    long fileSizeMB = storage.getPathInfo(hfilePath).getLength() / (1024 * 1024) + 1;
    props.setProperty("hoodie.metadata.file.cache.max.size.mb", String.valueOf(fileSizeMB * 2));

    HFileReaderFactory readerFactory = HFileReaderFactory.builder()
        .withStorage(storage)
        .withPath(hfilePath)
        .withProps(props)
        .build();

    HoodieNativeAvroHFileReader reader = HoodieNativeAvroHFileReader.builder()
        .readerFactory(readerFactory)
        .path(hfilePath)
        .build();

    HoodieSchema hoodieSchema = HoodieSchema.fromAvroSchema(SCHEMA);

    int totalRecordsRead = 0;
    int totalRecordsScanned = 0;

    try (ClosableIterator<IndexedRecord> iterator =
            reader.getIndexedRecordIterator(hoodieSchema, hoodieSchema, new java.util.HashMap<>())) {
      while (iterator.hasNext()) {
        IndexedRecord record = iterator.next();
        totalRecordsScanned++;

        // Extract key and check if it matches any prefix
        GenericRecord genericRecord = (GenericRecord) record;
        String key = genericRecord.get("key").toString();

        for (String fullPrefix : prefixKeys) {
          if (key.startsWith(fullPrefix)) {
            totalRecordsRead++;
            break;
          }
        }
      }
    }

    reader.close();

    long duration = System.currentTimeMillis() - startTime;
    int expectedRecords = prefixKeys.size() * ENTRIES_PER_PREFIX;

    LOG.info("Scenario C Results:");
    LOG.info("  Duration: {} ms", duration);
    LOG.info("  Records scanned: {}", totalRecordsScanned);
    LOG.info("  Records matched: {}", totalRecordsRead);
    LOG.info("  Expected records: {}", expectedRecords);
    LOG.info("  Match: {}", totalRecordsRead == expectedRecords);
  }
}
