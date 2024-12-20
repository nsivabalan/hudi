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

package org.apache.hudi;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieIndexMetadata;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.exception.HoodieMetadataIndexException;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.table.action.index.functional.BaseHoodieIndexClient;

import org.apache.spark.SparkToJavaUtils;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.common.config.HoodieMetadataConfig.RECORD_INDEX_ENABLE_PROP;
import static org.apache.hudi.common.util.ValidationUtils.checkArgument;
import static org.apache.hudi.index.expression.ExpressionIndexSparkFunctions.IDENTITY_FUNCTION;
import static org.apache.hudi.index.expression.HoodieExpressionIndex.EXPRESSION_OPTION;
import static org.apache.hudi.index.expression.HoodieExpressionIndex.RANGE_TYPE;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_BLOOM_FILTERS;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_EXPRESSION_INDEX_PREFIX;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_RECORD_INDEX;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_SECONDARY_INDEX;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_SECONDARY_INDEX_PREFIX;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.validateDataTypeForSecondaryIndex;

public class HoodieSparkIndexClient extends BaseHoodieIndexClient {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieSparkIndexClient.class);

  private Option<SparkSession> sparkSessionOpt = Option.empty();
  private Option<HoodieWriteConfig> writeConfigOpt = Option.empty();
  private Option<HoodieEngineContext> engineContextOpt = Option.empty();

  public HoodieSparkIndexClient(SparkSession sparkSession) {
    super();
    this.sparkSessionOpt = Option.of(sparkSession);
  }

  public HoodieSparkIndexClient(HoodieWriteConfig writeConfig, HoodieEngineContext engineContext) {
    super();
    this.writeConfigOpt = Option.of(writeConfig);
    this.engineContextOpt = Option.of(engineContext);
  }

  @Override
  public void create(HoodieTableMetaClient metaClient, String userIndexName, String indexType, Map<String, Map<String, String>> columns, Map<String, String> options,
                     Map<String, String> tableProperties) throws Exception {
    if (indexType.equals(PARTITION_NAME_SECONDARY_INDEX) || indexType.equals(PARTITION_NAME_BLOOM_FILTERS)
        || indexType.equals(PARTITION_NAME_COLUMN_STATS)) {
      createExpressionOrSecondaryIndex(metaClient, userIndexName, indexType, columns, options, tableProperties);
    } else {
      createRecordIndex(metaClient, userIndexName, indexType);
    }
  }

  private void createRecordIndex(HoodieTableMetaClient metaClient, String userIndexName, String indexType) {
    if (!userIndexName.equals(PARTITION_NAME_RECORD_INDEX)) {
      throw new HoodieIndexException("Record index should be named as record_index");
    }

    String fullIndexName = PARTITION_NAME_RECORD_INDEX;
    if (indexExists(metaClient, fullIndexName)) {
      throw new HoodieMetadataIndexException("Index already exists: " + userIndexName);
    }

    LOG.info("Creating index {} of using {}", fullIndexName, indexType);
    try (SparkRDDWriteClient writeClient = getWriteClient(metaClient)) {
      // generate index plan
      Option<String> indexInstantTime = doSchedule(writeClient, metaClient, fullIndexName, MetadataPartitionType.RECORD_INDEX);
      if (indexInstantTime.isPresent()) {
        // build index
        writeClient.index(indexInstantTime.get());
      } else {
        throw new HoodieMetadataIndexException("Scheduling of index action did not return any instant.");
      }
    } catch (Throwable t) {
      drop(metaClient, fullIndexName, Option.empty());
      throw t;
    }
  }

  private SparkRDDWriteClient getWriteClient(HoodieTableMetaClient metaClient) {
    if (writeConfigOpt.isPresent()) {
      HoodieWriteConfig localWriteConfig = HoodieWriteConfig.newBuilder()
          .withPath(metaClient.getBasePath())
          .withProperties(writeConfigOpt.get().getProps())
          .withEmbeddedTimelineServerEnabled(false)
          .withAutoCommit(false)
          .withEngineType(EngineType.SPARK).build();
      return new SparkRDDWriteClient(engineContextOpt.get(), localWriteConfig, Option.empty());
    } else {
      TypedProperties typedProperties = metaClient.getTableConfig().getProps();
      SparkToJavaUtils.convertToJavaMap(sparkSessionOpt.get().sqlContext().getAllConfs()).forEach((k, v) -> {
        if (k.startsWith("hoodie.")) {
          typedProperties.put(k, v);
        }
      });

      HoodieWriteConfig localWriteConfig = HoodieWriteConfig.newBuilder()
          .withPath(metaClient.getBasePath())
          .withProperties(typedProperties)
          .withEmbeddedTimelineServerEnabled(false)
          .withAutoCommit(false)
          .withEngineType(EngineType.SPARK).build();
      return new SparkRDDWriteClient(engineContextOpt.get(), localWriteConfig, Option.empty());
    }
  }

  @Override
  public void createOrUpdateColumnStatsIndexDefinition(HoodieTableMetaClient metaClient, List<String> columnsToIndex) {

    String fullIndexName = PARTITION_NAME_COLUMN_STATS;

    HoodieIndexDefinition indexDefinition = new HoodieIndexDefinition(fullIndexName, RANGE_TYPE, RANGE_TYPE,
        columnsToIndex, Collections.EMPTY_MAP);
    LOG.info("Registering Or Updating the index " + fullIndexName);
    register(metaClient, indexDefinition);
  }

  private void createExpressionOrSecondaryIndex(HoodieTableMetaClient metaClient, String userIndexName, String indexType,
                                                Map<String, Map<String, String>> columns, Map<String, String> options, Map<String, String> tableProperties) throws Exception {
    String fullIndexName = indexType.equals(PARTITION_NAME_SECONDARY_INDEX)
        ? PARTITION_NAME_SECONDARY_INDEX_PREFIX + userIndexName
        : PARTITION_NAME_EXPRESSION_INDEX_PREFIX + userIndexName;
    if (indexExists(metaClient, fullIndexName)) {
      throw new HoodieMetadataIndexException("Index already exists: " + userIndexName);
    }
    checkArgument(columns.size() == 1, "Only one column can be indexed for functional or secondary index.");

    if (!isEligibleForIndexing(metaClient, indexType, tableProperties, columns)) {
      throw new HoodieMetadataIndexException("Not eligible for indexing: " + indexType + ", indexName: " + userIndexName);
    }

    HoodieIndexDefinition indexDefinition = new HoodieIndexDefinition(fullIndexName, indexType, options.getOrDefault(EXPRESSION_OPTION, IDENTITY_FUNCTION),
        new ArrayList<>(columns.keySet()), options);
    if (!metaClient.getTableConfig().getRelativeIndexDefinitionPath().isPresent()
        || !metaClient.getIndexMetadata().isPresent()
        || !metaClient.getIndexMetadata().get().getIndexDefinitions().containsKey(fullIndexName)) {
      LOG.info("Index definition is not present. Registering the index first");
      register(metaClient, indexDefinition);
    }

    ValidationUtils.checkState(metaClient.getIndexMetadata().isPresent(), "Index definition is not present");

    LOG.info("Creating index {} of using {}", fullIndexName, indexType);
    Option<HoodieIndexDefinition> expressionIndexDefinitionOpt = Option.ofNullable(indexDefinition);
    try (SparkRDDWriteClient writeClient = getWriteClient(metaClient)) {
      MetadataPartitionType partitionType = indexType.equals(PARTITION_NAME_SECONDARY_INDEX) ? MetadataPartitionType.SECONDARY_INDEX : MetadataPartitionType.EXPRESSION_INDEX;
      // generate index plan
      Option<String> indexInstantTime = doSchedule(writeClient, metaClient, fullIndexName, partitionType);
      if (indexInstantTime.isPresent()) {
        // build index
        writeClient.index(indexInstantTime.get());
      } else {
        throw new HoodieMetadataIndexException("Scheduling of index action did not return any instant.");
      }
    } catch (Throwable t) {
      drop(metaClient, fullIndexName, Option.ofNullable(indexDefinition));
      throw t;
    }
  }

  private void drop(HoodieTableMetaClient metaClient, String indexName, Option<HoodieIndexDefinition> indexDefinitionOpt) {
    LOG.info("Dropping index {}", indexName);
    try (SparkRDDWriteClient writeClient = getWriteClient(metaClient)) {
      writeClient.dropIndex(Collections.singletonList(indexName));
    }
  }

  @Override
  public void drop(HoodieTableMetaClient metaClient, String indexName, boolean ignoreIfNotExists) {
    LOG.info("Dropping index {}", indexName);
    Option<HoodieIndexDefinition> indexDefinitionOpt = metaClient.getIndexMetadata()
        .map(HoodieIndexMetadata::getIndexDefinitions)
        .map(definition -> definition.get(indexName));
    try (SparkRDDWriteClient writeClient = getWriteClient(metaClient)) {
      writeClient.dropIndex(Collections.singletonList(indexName));
    }
  }

  private static Option<String> doSchedule(SparkRDDWriteClient<HoodieRecordPayload> client, HoodieTableMetaClient metaClient, String indexName, MetadataPartitionType partitionType) {
    List<MetadataPartitionType> partitionTypes = Collections.singletonList(partitionType);
    if (metaClient.getTableConfig().getMetadataPartitions().isEmpty()) {
      throw new HoodieException("Metadata table is not yet initialized. Initialize FILES partition before any other partition " + Arrays.toString(partitionTypes.toArray()));
    }
    return client.scheduleIndexing(partitionTypes, Collections.singletonList(indexName));
  }

  private static boolean indexExists(HoodieTableMetaClient metaClient, String indexName) {
    return metaClient.getTableConfig().getMetadataPartitions().stream().anyMatch(partition -> partition.equals(indexName));
  }

  /*private static Map<String, String> buildWriteConfig(HoodieTableMetaClient metaClient, Option<HoodieIndexDefinition> indexDefinitionOpt, Option<String> indexTypeOpt) {
    Map<String, String> writeConfig = new HashMap<>();
    if (metaClient.getTableConfig().isMetadataTableAvailable()) {
      writeConfig.put(HoodieWriteConfig.WRITE_CONCURRENCY_MODE.key(), WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL.name());
      writeConfig.putAll(JavaConverters.mapAsJavaMapConverter(HoodieCLIUtils.getLockOptions(metaClient.getBasePath().toString(),
          metaClient.getBasePath().toUri().getScheme(), new TypedProperties())).asJava());

      // [HUDI-7472] Ensure write-config contains the existing MDT partition to prevent those from getting deleted
      metaClient.getTableConfig().getMetadataPartitions().forEach(partitionPath -> {
        if (partitionPath.equals(MetadataPartitionType.RECORD_INDEX.getPartitionPath())) {
          writeConfig.put(RECORD_INDEX_ENABLE_PROP.key(), "true");
        }

        if (partitionPath.equals(MetadataPartitionType.BLOOM_FILTERS.getPartitionPath())) {
          writeConfig.put(ENABLE_METADATA_INDEX_BLOOM_FILTER.key(), "true");
        }

        if (partitionPath.equals(MetadataPartitionType.COLUMN_STATS.getPartitionPath())) {
          writeConfig.put(ENABLE_METADATA_INDEX_COLUMN_STATS.key(), "true");
        }
      });

      if (indexTypeOpt.isPresent()) {
        String indexType = indexTypeOpt.get();
        if (indexType.equals(PARTITION_NAME_RECORD_INDEX)) {
          writeConfig.put(RECORD_INDEX_ENABLE_PROP.key(), "true");
        }
      }
    }

    indexDefinitionOpt.ifPresent(indexDefinition ->
        HoodieIndexingConfig.fromIndexDefinition(indexDefinition).getProps().forEach((key, value) -> writeConfig.put(key.toString(), value.toString())));
    return writeConfig;
  }*/

  private static boolean isEligibleForIndexing(HoodieTableMetaClient metaClient, String indexType, Map<String, String> options, Map<String, Map<String, String>> columns) throws Exception {
    if (!validateDataTypeForSecondaryIndex(new ArrayList<>(columns.keySet()), new TableSchemaResolver(metaClient).getTableAvroSchema())) {
      return false;
    }
    // for secondary index, record index is a must
    if (indexType.equals(PARTITION_NAME_SECONDARY_INDEX)) {
      // either record index is enabled or record index partition is already present
      return metaClient.getTableConfig().getMetadataPartitions().stream().anyMatch(partition -> partition.equals(MetadataPartitionType.RECORD_INDEX.getPartitionPath()))
          || Boolean.parseBoolean(options.getOrDefault(RECORD_INDEX_ENABLE_PROP.key(), RECORD_INDEX_ENABLE_PROP.defaultValue().toString()));
    }
    return true;
  }
}
