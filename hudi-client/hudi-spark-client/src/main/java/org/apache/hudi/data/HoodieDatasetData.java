package org.apache.hudi.data;

import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.function.SerializableFunction;
import org.apache.hudi.common.function.SerializablePairFunction;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.storage.StorageLevel;

import java.util.Iterator;
import java.util.List;

public class HoodieDatasetData<T> implements HoodieData {

  private final Dataset<T> dataset;


  private HoodieDatasetData(Dataset<T> dataset) {
    this.dataset = dataset;
  }

  /**
   * @param data a {@link JavaRDD} of objects in type T.
   * @param <T>     type of object.
   * @return a new instance containing the {@link JavaRDD<T>} reference.
   */
  public static <T> HoodieDatasetData<T> of(Dataset<T> data) {
    return new HoodieDatasetData(data);
  }

  /**
   * @param data        a {@link List} of objects in type T.
   * @param context     {@link HoodieSparkEngineContext} to use.
   * @param parallelism parallelism for the {@link JavaRDD<T>}.
   * @param <T>         type of object.
   * @return a new instance containing the {@link JavaRDD<T>} instance.
   */
  public static <T> HoodieDatasetData<T> of(
      List<T> data, HoodieSparkEngineContext context, int parallelism, Encoder encoder) {
    return new HoodieDatasetData(context.getSqlContext().sparkSession().createDataset(data, encoder));
  }

  /**
   * @param hoodieData {@link HoodieJavaRDD <T>} instance containing the {@link JavaRDD} of objects.
   * @param <T>        type of object.
   * @return the a {@link JavaRDD} of objects in type T.
   */
  public static <T> Dataset<T> getDataset(HoodieData<T> hoodieData) {
    return ((HoodieDatasetData<T>) hoodieData).dataset;
  }

  /* Unsupported in Dataset
  public static <K, V> JavaPairRDD<K, V> getJavaRDD(HoodiePairData<K, V> hoodieData) {
    return ((HoodieJavaPairRDD<K, V>) hoodieData).get();
  }
*/

  @Override
  public int getId() {
    return dataset.rdd().id();
  }

  @Override
  public void persist(String level) {
    dataset.persist(StorageLevel.fromString(level));
  }

  @Override
  public void persist(String level, HoodieEngineContext engineContext, HoodieDataCacheKey cacheKey) {
    dataset.persist(StorageLevel.fromString(level));
  }

  @Override
  public void unpersist() {
   dataset.unpersist();
  }

  @Override
  public boolean isEmpty() {
    return dataset.isEmpty();
  }

  @Override
  public long count() {
    return dataset.count();
  }

  @Override
  public int getNumPartitions() {
    return dataset.toJavaRDD().getNumPartitions();
  }

  @Override
  public int deduceNumPartitions() {
    return dataset.toJavaRDD().getNumPartitions();
  }

  @Override
  // needs an encoder. check below.
  public HoodieData flatMap(SerializableFunction func) {
    return null;
  }

  @Override
  // needs an encoder. check below.
  public HoodieData mapPartitions(SerializableFunction func, boolean preservesPartitioning) {
    return null;
  }

  @Override
  // needs an encoder. check below.
  public HoodieData map(SerializableFunction func) {
    return null;
  }

  @Override
  public HoodieData distinct() {
    return HoodieDatasetData.of(dataset.distinct());
  }

  @Override
  public HoodieData distinct(int parallelism) {
    // don't have an api in Dataset which takes in parallelism.
    return HoodieDatasetData.of(dataset.distinct());
  }

  @Override
  // needs an column or column exp as filter
  public HoodieData filter(SerializableFunction filterFunc) {
    // in dataaset, we can only filter based on column expr.
    // "age > 15"
    return HoodieDatasetData.of(dataset);
  }

  @Override
  public HoodieData union(HoodieData other) {
    return HoodieDatasetData.of(dataset.union(((HoodieDatasetData<T>) other).dataset));
  }

  @Override
  public List collectAsList() {
    return dataset.collectAsList();
  }

  @Override
  public HoodieData repartition(int parallelism) {
    return HoodieDatasetData.of(dataset.repartition(parallelism));
  }

  /** Unsupported */
  @Override
  public HoodiePairData mapToPair(SerializablePairFunction func) {
    return null;
  }

  /** Unsupported */
  @Override
  public HoodiePairData flatMapToPair(SerializableFunction func) {
    return null;
  }

  //@Override
  // Needs an encoder
  public <O> HoodieData<O> flatMap(SerializableFunction<T, Iterator<O>> func, Encoder<O> encoder) {
    return HoodieDatasetData.of(dataset.flatMap(new FlatMapFunction<T, O>() {
      @Override
      public Iterator<O> call(T t) throws Exception {
        return func.apply(t);
      }
    }, encoder));
  }

  //@Override
  // Needs an encoder
  public <O> HoodieData<O> mapPartitions(SerializableFunction<Iterator<T>,
      Iterator<O>> func, boolean preservesPartitioning, Encoder<O> encoder) {
    return HoodieDatasetData.of(dataset.mapPartitions(new MapPartitionsFunction<T, O>() {
      @Override
      public Iterator<O> call(Iterator<T> iterator) throws Exception {
        return func.apply(iterator);
      }
    }, encoder));
  }

  //@Override
  // Needs an encoder
  public <O> HoodieData<O> map(SerializableFunction<T, O> func, Encoder<O> encoder) {
    return HoodieDatasetData.of(dataset.map(new MapFunction<T, O>() {
      @Override
      public O call(T t) throws Exception {
        return func.apply(t);
      }
    }, encoder));
  }
}
