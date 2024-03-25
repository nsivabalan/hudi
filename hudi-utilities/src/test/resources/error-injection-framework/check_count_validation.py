import os
import json
import pyspark

from pyspark.sql import SparkSession

table_name = os.environ["TABLE_NAME"] 
base_path = f'{os.environ["BASE_PATH"]}/{table_name}'
input_path = os.environ["INPUT_DATA_PATH"] 

spark = SparkSession.builder.appName('hudi-testing') \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()

sc = spark.sparkContext
spark.read.parquet(input_path+"/*").createOrReplaceTempView("input_data")

input_df = spark.sql("select count(distinct partition, key) c from input_data")
input_count = (input_df.toJSON().map(lambda j: json.loads(j)).collect()[0]['c'])

hudi_df = spark.read.format("hudi").load(base_path).cache()

# print(hudi_df.cache().count())
hudi_df.createOrReplaceTempView("hudi_table")

output_df = spark.sql("select count(*) c from hudi_table")
# print(output_df.toJSON().map(lambda j: json.loads(j)).collect()[0]['c'])

# partition_count = spark.sql("select count(partition, key) c from hudi_table")
hudi_count = (output_df.toJSON().map(lambda j: json.loads(j)).collect()[0]['c'])
print(f"Partition key counts- {hudi_count} {input_count}")
if input_count==hudi_count:
    print("Input and HUDI table count matches.")
else:
    raise ValueError("Input and HUDI table count mismatches.")
