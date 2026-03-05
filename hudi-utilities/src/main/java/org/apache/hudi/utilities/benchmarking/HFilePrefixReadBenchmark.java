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
import org.apache.hudi.avro.model.HoodieMetadataColumnStats;
import org.apache.hudi.avro.model.StringWrapper;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.hash.ColumnIndexID;
import org.apache.hudi.common.util.hash.FileIndexID;
import org.apache.hudi.common.util.hash.PartitionIndexID;
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
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.UUID;

import static org.apache.hudi.metadata.HoodieTableMetadataUtil.getColumnStatsIndexPartitionIdentifier;

/**
 * Benchmark tool to measure HFile prefix read performance for column stats index.
 *
 * Creates an HFile with 1M entries in the same format as the metadata table's column_stats partition:
 * - 365 partitions (daily partitions in yyyy-MM-dd format)
 * - ~2,740 files per partition
 * - 1 column stats entry per file (for column "tenantId")
 * - Keys: columnIndexID + partitionIndexID + fileIndexID (base64 encoded)
 * - Each prefix lookup returns ~2,740 entries (all files in one partition)
 *
 * Three benchmark scenarios (select with --scenario):
 * A) Prefix read without downloading entire file (no cache)
 * B) Download entire file upfront, then do prefix read (with cache)
 * C) Download entire file upfront, then iteratively read all entries and filter (with cache + scan)
 *
 * NOTE: Run each scenario separately to avoid cache interference between scenarios.
 * Use --scenario A, B, or C to run a single scenario, or --scenario all to run all three.
 */
public class HFilePrefixReadBenchmark {

  private static final Logger LOG = LoggerFactory.getLogger(HFilePrefixReadBenchmark.class);

  // Constants for HFile generation
  private static final int TOTAL_ENTRIES = 1_000_000;
  private static final int NUM_PARTITIONS = 400; // One year of daily partitions
  private static final int FILES_PER_PARTITION = TOTAL_ENTRIES / NUM_PARTITIONS; // ~2,740 files per partition
  private static final int ENTRIES_PER_PREFIX = FILES_PER_PARTITION; // 1 entry per file
  private static final String COLUMN_NAME = "tenantId";

  // Use the actual HoodieMetadataColumnStats schema
  private static final Schema COLUMN_STATS_SCHEMA = HoodieMetadataColumnStats.SCHEMA$;

  public static class Config implements Serializable {
    @Parameter(names = {"--output-dir", "-o"}, description = "Output directory for HFile", required = true)
    public String outputDir = "/tmp/hfile_bench/";

    @Parameter(names = {"--num-keys", "-n"}, description = "Number of prefix keys to test (comma-separated, e.g., '1,10,30,50')")
    public String numKeys = "1,10,30,50";

    @Parameter(names = {"--scenario", "-s"}, description = "Scenario to run: A (no cache), B (with cache + prefix read), C (with cache + full scan). Default: all")
    public String scenario = "all";

    @Parameter(names = {"--help", "-h"}, help = true)
    public Boolean help = false;

    @Override
    public String toString() {
      return "HFilePrefixReadBenchmark {\n"
          + "   --output-dir " + outputDir + ",\n"
          + "   --num-keys " + numKeys + ",\n"
          + "   --scenario " + scenario + "\n"
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

    // Generate filename with first and last partition date and current timestamp
    String firstPartition = generatePartitionName(0);
    String lastPartition = generatePartitionName(NUM_PARTITIONS - 1);
    String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmm"));
    String filename = String.format("hfile_colstats_%s_partition_%s_to_%s_%s.hfile",
        COLUMN_NAME, firstPartition, lastPartition, timestamp);
    this.hfilePath = new StoragePath(outputDirPath, filename);

    LOG.info("HFile will be created at: {}", hfilePath);
    LOG.info("Column: {}", COLUMN_NAME);
    LOG.info("Partitions: {} to {} ({} total)", firstPartition, lastPartition, NUM_PARTITIONS);
    LOG.info("Entries per partition: {}", ENTRIES_PER_PREFIX);
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
    LOG.info("Starting HFile Prefix Read Benchmark (Column Stats Index Format)");
    LOG.info("Config: {}", cfg);

    // Step 1: Create HFile with 1M entries
    LOG.info("Step 1: Creating HFile with {} entries ({} partitions, {} entries per partition)",
        TOTAL_ENTRIES, NUM_PARTITIONS, ENTRIES_PER_PREFIX);
    LOG.info("Column: {}", COLUMN_NAME);
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
   * Key format: columnIndexID + partitionIndexID + fileIndexID (all base64 encoded)
   * Uses real column stats index key format with:
   * - Column: tenantId (fixed)
   * - Partitions: 365 daily partitions (yyyy-MM-dd format)
   * - Files: ~2,740 files per partition with Hoodie naming convention
   * - 1 entry per file (representing column stats for that file)
   */
  private void createHFile() throws IOException {
    long startTime = System.currentTimeMillis();

    // Create Hudi HFileContext
    HFileContext context = HFileContext.builder()
        .blockSize(1024 * 1024) // 64KB blocks
        .compressionCodec(CompressionCodec.GZIP)
        .build();

    // Create output stream for the HFile
    OutputStream outputStream = storage.create(hfilePath);

    // Use Hudi's HFileWriterImpl
    HFileWriterImpl writer = new HFileWriterImpl(context, outputStream);

    // Write schema metadata
    writer.appendFileInfo("schema", COLUMN_STATS_SCHEMA.toString().getBytes(StandardCharsets.UTF_8));
    HoodieSchema hoodieSchema = HoodieSchema.fromAvroSchema(COLUMN_STATS_SCHEMA);

    // Step 1: Collect all entries in a TreeMap to ensure sorted order (required by HFile)
    LOG.info("Step 1a: Collecting entries in sorted order...");
    TreeMap<String, HoodieMetadataColumnStats> sortedEntries = new TreeMap<>();
    int entriesGenerated = 0;

    // For each partition, generate one entry per file
    for (int partitionIdx = 0; partitionIdx < NUM_PARTITIONS; partitionIdx++) {
      String partitionName = generatePartitionName(partitionIdx);

      // Generate FILES_PER_PARTITION files for this partition, with 1 entry per file
      for (int fileIdx = 0; fileIdx < FILES_PER_PARTITION; fileIdx++) {
        String fileName = generateHoodieFileName(partitionIdx * FILES_PER_PARTITION + fileIdx);

        // Create one entry for this file
        String key = getColumnStatsIndexKey(partitionName, COLUMN_NAME, fileName);

        // Create HoodieMetadataColumnStats record (realistic column stats data)
        HoodieMetadataColumnStats colStats = createColumnStatsRecord(fileName, partitionIdx, fileIdx);

        sortedEntries.put(key, colStats);
        entriesGenerated++;

        if (entriesGenerated % 100000 == 0) {
          LOG.info("Generated {} entries... (partition: {})", entriesGenerated, partitionName);
        }
      }
    }

    LOG.info("Step 1b: Generated {} entries, now writing to HFile in sorted order...", sortedEntries.size());

    // Step 2: Write all entries to HFile in sorted order
    String minRecordKey = null;
    String maxRecordKey = null;
    int entriesWritten = 0;

    for (java.util.Map.Entry<String, HoodieMetadataColumnStats> entry : sortedEntries.entrySet()) {
      String key = entry.getKey();
      HoodieMetadataColumnStats colStats = entry.getValue();

      // Track min and max record keys
      if (minRecordKey == null) {
        minRecordKey = key;
      }
      maxRecordKey = key;

      writer.append(key, HoodieAvroUtils.avroToBytes(colStats));
      entriesWritten++;

      if (entriesWritten % 100000 == 0) {
        LOG.info("Written {} entries to HFile...", entriesWritten);
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
   * Generates a partition name (date string) for the given index.
   * Uses yyyy-MM-dd format starting from 2024-01-01
   */
  private String generatePartitionName(int index) {
    LocalDate startDate = LocalDate.of(2024, 1, 1);
    LocalDate partitionDate = startDate.plusDays(index);
    return partitionDate.format(DateTimeFormatter.ISO_LOCAL_DATE);
  }

  /**
   * Generates a Hoodie base file name.
   * Format: <fileId>_<writeToken>_<instantTime>.parquet
   */
  private String generateHoodieFileName(int index) {
    String fileId = UUID.randomUUID().toString();
    String writeToken = "1-0-1";
    String instantTime = String.format("20240101%06d", index);
    return String.format("%s_%s_%s.parquet", fileId, writeToken, instantTime);
  }

  public static String getColumnStatsIndexKey(String partitionName, String colName, String fileName) {
    final PartitionIndexID partitionIndexID = new PartitionIndexID(getColumnStatsIndexPartitionIdentifier(partitionName));
    final FileIndexID fileIndexID = new FileIndexID(fileName);
    final ColumnIndexID columnIndexID = new ColumnIndexID(colName);

    return columnIndexID.asBase64EncodedString()
        .concat(partitionIndexID.asBase64EncodedString())
        .concat(fileIndexID.asBase64EncodedString());
  }

  /**
   * Generates the prefix for lookup: columnIndexID + partitionIndexID
   */
  private String generateLookupPrefix(String partitionName) {
    final PartitionIndexID partitionIndexID = new PartitionIndexID(getColumnStatsIndexPartitionIdentifier(partitionName));
    final ColumnIndexID columnIndexID = new ColumnIndexID(COLUMN_NAME);

    return columnIndexID.asBase64EncodedString()
        .concat(partitionIndexID.asBase64EncodedString());
  }

  /**
   * Creates a realistic HoodieMetadataColumnStats record for a file.
   * This matches the actual format stored in the metadata table's column_stats partition.
   */
  private HoodieMetadataColumnStats createColumnStatsRecord(String fileName, int partitionIdx, int fileIdx) {
    // Generate realistic min/max values for a string column (tenantId)
    // Use partition and file index to create varied but deterministic values
    String minTenantId = String.format("tenant_%05d", partitionIdx * 100);
    String maxTenantId = String.format("tenant_%05d", partitionIdx * 100 + 99);

    return HoodieMetadataColumnStats.newBuilder()
        .setFileName(fileName)
        .setColumnName(COLUMN_NAME)
        .setMinValue(StringWrapper.newBuilder().setValue(minTenantId).build())
        .setMaxValue(StringWrapper.newBuilder().setValue(maxTenantId).build())
        .setValueCount(10000L)  // Assume 10K rows per file
        .setNullCount(100L)     // Assume 100 nulls per file
        .setTotalSize(1024L * 1024L)  // 1MB compressed
        .setTotalUncompressedSize(2048L * 1024L)  // 2MB uncompressed
        .setIsDeleted(false)
        .setIsTightBound(true)
        .build();
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
   * Distributes them evenly across the partition range.
   * Returns columnIndexID + partitionIndexID prefixes in SORTED order.
   *
   * NOTE: The prefix keys MUST be sorted for HFile prefix lookup to work correctly.
   */
  private List<String> selectPrefixKeys(int count) {
    List<String> prefixes = new ArrayList<>();
    int step = NUM_PARTITIONS / count;
    if (step == 0) {
      step = 1;
    }

    for (int i = 0; i < count && i * step < NUM_PARTITIONS; i++) {
      String partitionName = generatePartitionName(i * step);
      prefixes.add(generateLookupPrefix(partitionName));
    }

    // Sort the prefixes to ensure they're in the correct order for HFile lookup
    java.util.Collections.sort(prefixes);

    return prefixes;
  }

  /**
   * Runs the selected benchmark scenario(s)
   */
  private void runBenchmarks(List<String> prefixKeys) throws Exception {
    String scenario = cfg.scenario.toUpperCase();

    switch (scenario) {
      case "A":
        LOG.info("\n--- Scenario A: Prefix read without downloading entire file ---");
        runScenarioA(prefixKeys);
        break;

      case "B":
        LOG.info("\n--- Scenario B: Download entire file, then prefix read ---");
        runScenarioB(prefixKeys);
        break;

      case "C":
        LOG.info("\n--- Scenario C: Download entire file, then iterative read with filter ---");
        runScenarioC(prefixKeys);
        break;

      case "ALL":
        LOG.info("\n--- Running all scenarios (note: cache may affect results) ---");
        LOG.info("\n--- Scenario A: Prefix read without downloading entire file ---");
        runScenarioA(prefixKeys);

        LOG.info("\n--- Scenario B: Download entire file, then prefix read ---");
        runScenarioB(prefixKeys);

        LOG.info("\n--- Scenario C: Download entire file, then iterative read with filter ---");
        runScenarioC(prefixKeys);
        break;

      default:
        throw new IllegalArgumentException("Invalid scenario: " + cfg.scenario + ". Valid values: A, B, C, all");
    }
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

    HoodieSchema hoodieSchema = HoodieSchema.fromAvroSchema(COLUMN_STATS_SCHEMA);
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

    HoodieSchema hoodieSchema = HoodieSchema.fromAvroSchema(COLUMN_STATS_SCHEMA);

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

    HoodieSchema hoodieSchema = HoodieSchema.fromAvroSchema(COLUMN_STATS_SCHEMA);

    int totalRecordsRead = 0;
    int totalRecordsScanned = 0;

    // In Scenario C, we scan all records and count those matching our column
    // This simulates the worst-case scenario of scanning entire file
    try (ClosableIterator<IndexedRecord> iterator =
            reader.getIndexedRecordIterator(hoodieSchema, hoodieSchema, new java.util.HashMap<>())) {
      while (iterator.hasNext()) {
        IndexedRecord record = iterator.next();
        totalRecordsScanned++;

        // Check if this record is for our target column
        GenericRecord genericRecord = (GenericRecord) record;
        Object columnNameObj = genericRecord.get("columnName");

        if (columnNameObj != null && columnNameObj.toString().equals(COLUMN_NAME)) {
          totalRecordsRead++;
        }
      }
    }

    reader.close();

    long duration = System.currentTimeMillis() - startTime;
    // In Scenario C, we scan ALL records and count those matching the column
    // Since all 1M records are for COLUMN_NAME, we expect all of them
    int expectedRecords = TOTAL_ENTRIES;
    int expectedMatches = TOTAL_ENTRIES;  // All records match since they're all for the same column

    LOG.info("Scenario C Results:");
    LOG.info("  Duration: {} ms", duration);
    LOG.info("  Records scanned: {}", totalRecordsScanned);
    LOG.info("  Records matched (for column '{}'): {}", COLUMN_NAME, totalRecordsRead);
    LOG.info("  Expected scanned: {}", expectedRecords);
    LOG.info("  Expected matched: {}", expectedMatches);
    LOG.info("  Match: {}", totalRecordsRead == expectedMatches && totalRecordsScanned == expectedRecords);
  }
}
