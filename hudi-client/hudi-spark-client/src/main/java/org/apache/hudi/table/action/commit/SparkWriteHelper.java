package org.apache.hudi.table.action.commit;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.function.SerializableFunctionUnchecked;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.model.HoodieSparkBeanRecord;
import org.apache.hudi.data.HoodieDatasetData;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.HoodieTable;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

public class SparkWriteHelper<T, R> extends BaseWriteHelper<T, HoodieData<HoodieSparkBeanRecord>,
    HoodieData<HoodieKey>, HoodieData<WriteStatus>, R> {

  protected SparkWriteHelper(
      SerializableFunctionUnchecked<HoodieData<HoodieSparkBeanRecord>, Integer> partitionNumberExtractor) {
    super(partitionNumberExtractor);
  }

  @Override
  protected HoodieData<HoodieSparkBeanRecord> tag(HoodieData<HoodieSparkBeanRecord> dedupedRecords, HoodieEngineContext context,
                                                  HoodieTable<T, HoodieData<HoodieSparkBeanRecord>, HoodieData<HoodieKey>, HoodieData<WriteStatus>> table) {
    return null;
  }

  @Override
  public HoodieData<HoodieSparkBeanRecord> deduplicateRecords(HoodieData<HoodieSparkBeanRecord> records, HoodieIndex<?, ?> index, int parallelism, String schema, TypedProperties props,
                                                              HoodieRecordMerger merger) {
    // in rdd, we do mapToPair and then reduce By Key.
    // here with dataset, we might have to do something like below.
    Dataset<HoodieSparkBeanRecord> datasetRecords = HoodieDatasetData.getDataset(records);
    return HoodieDatasetData.of(datasetRecords.groupByKey(new MapFunction<HoodieSparkBeanRecord, String>() {
      @Override
      public String call(HoodieSparkBeanRecord hoodieSparkBeanRecord) throws Exception {
        return hoodieSparkBeanRecord.getKey().toString();
      }
    }, Encoders.STRING()).reduceGroups(new ReduceFunction<HoodieSparkBeanRecord>() {
      @Override
      public HoodieSparkBeanRecord call(HoodieSparkBeanRecord hoodieSparkBeanRecord, HoodieSparkBeanRecord t1) throws Exception {
        // compare preocombine value and return
        return hoodieSparkBeanRecord;
      }
    }));
  }


  public Dataset<HoodieSparkBeanRecord> repartition(Dataset<HoodieSparkBeanRecord> records, int outputSparkPartitions, String[] sortColumnNames, SparkSession spark) {

    records.toDF()
        .sort(Arrays.stream(sortColumnNames).map(Column::new).toArray(Column[]::new))
        .coalesce(outputSparkPartitions);

  }
}
