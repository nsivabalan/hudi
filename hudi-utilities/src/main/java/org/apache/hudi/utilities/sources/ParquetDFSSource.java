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

package org.apache.hudi.utilities.sources;

import org.apache.hudi.SparkAdapterSupport$;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.utilities.config.ParquetDFSSourceConfig;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.helpers.DFSPathSelector;

import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.apache.hudi.common.util.ConfigUtils.getBooleanWithAltKeys;

/**
 * DFS Source that reads parquet data.
 */
public class ParquetDFSSource extends RowSource {

  private static final Logger LOG = LoggerFactory.getLogger(ParquetDFSSource.class);

  private final DFSPathSelector pathSelector;

  public ParquetDFSSource(TypedProperties props, JavaSparkContext sparkContext, SparkSession sparkSession,
                          SchemaProvider schemaProvider) {
    super(props, sparkContext, sparkSession, schemaProvider);
    this.pathSelector = DFSPathSelector.createSourceSelector(props, this.sparkContext.hadoopConfiguration());
  }

  @Override
  public Pair<Option<Dataset<Row>>, String> fetchNextBatch(Option<String> lastCkptStr, long sourceLimit) {
    Pair<Option<String>, String> selectPathsWithMaxModificationTime =
        pathSelector.getNextFilePathsAndMaxModificationTime(sparkContext, lastCkptStr, sourceLimit);
    return selectPathsWithMaxModificationTime.getLeft()
        .map(pathStr -> Pair.of(Option.of(fromFiles(pathStr)), selectPathsWithMaxModificationTime.getRight()))
        .orElseGet(() -> Pair.of(Option.empty(), selectPathsWithMaxModificationTime.getRight()));
  }

  private Dataset<Row> fromFiles(String pathStr) {
    boolean mergeSchemaOption = getBooleanWithAltKeys(this.props, ParquetDFSSourceConfig.PARQUET_DFS_MERGE_SCHEMA);
    Dataset<Row> toReturn = sparkSession.read().option("mergeSchema", mergeSchemaOption).parquet(pathStr.split(","));

    ExpressionEncoder<Row> encoder = getEncoder(toReturn.schema());
    return toReturn.mapPartitions((MapPartitionsFunction<Row, Row>) input -> {
      List<Row> rows = new ArrayList<>();
      while (input.hasNext()) {
        rows.add(input.next());
      }
      LOG.info("XXX Within map Partitions in fetchNextBatch with total records  " + rows.size() + ", for stageId "
          + TaskContext.get().stageId() + " stage Attempt No " + TaskContext.get().stageAttemptNumber()
          + " task/spark partitiondId " + TaskContext.getPartitionId() + ", task Attempt No " + TaskContext.get().attemptNumber()
          + ", task attempt Id " + TaskContext.get().taskAttemptId());
      return rows.iterator();
    }, encoder);

    /*Dataset<Row> toReturn1 = sparkSession.createDataFrame(toReturn.rdd().toJavaRDD().mapPartitions((FlatMapFunction<Iterator<Row>, Row>) rowIterator -> {
      List<Row> rows = new ArrayList<>();
      while (rowIterator.hasNext()) {
        rows.add(rowIterator.next());
      }
      LOG.info("XXX Within map Partitions in fetchNextBatch with total records  " + rows.size() + ", for stageId "
          + TaskContext.get().stageId() + " stage Attempt No " + TaskContext.get().stageAttemptNumber()
          + " task/spark partitiondId " + TaskContext.getPartitionId() + ", task Attempt No " + TaskContext.get().attemptNumber()
          + ", task attempt Id " + TaskContext.get().taskAttemptId());
      return rowIterator;
    }).rdd(), Row.class);*/
    // return toReturn;
  }

  private static ExpressionEncoder getEncoder(StructType schema) {
    return SparkAdapterSupport$.MODULE$.sparkAdapter().getCatalystExpressionUtils().getEncoder(schema);
  }
}
