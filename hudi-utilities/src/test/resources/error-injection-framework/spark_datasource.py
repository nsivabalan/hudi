
import os
import pyspark

from pyspark.sql import SparkSession
from jproperties import Properties


print(os.environ)
input_data_path = os.environ["INPUT_DATA_PATH"]
table_name = os.environ["TABLE_NAME"]
base_path = f'{os.environ["BASE_PATH"]}/{table_name}'
properties_path = os.environ["PROPERTIES_PATH"]
hudi_op = os.environ["OP"]
table_type = os.environ["TABLE_TYPE"]
run_dir = os.environ["RUN_DIR"]

checkpoint_file = f"{run_dir}/checkPointFile"

def get_data_partitions():
    data_files = {}
    for x in os.walk(input_data_path):
        if input_data_path != x[0] and len(x[2]) > 0:

            part = int(x[0].split("/")[-1])
            data_files[part] = []
            for f in x[2]:
                if f.endswith(".parquet"):
                    data_files[part].append(f"{input_data_path}/{part}/{f}")

    return sorted(list(data_files.items()))

def get_properties():
    configs = Properties()
    with open(properties_path, "rb") as read_prop:
        configs.load(read_prop)
    prop_view = configs.items()
    print(type(prop_view))
    hudi_options = {
        "hoodie.table.name": table_name,
        "hoodie.datasource.write.table.name": table_name,
    }
    for item in prop_view:
        hudi_options[item[0]] = item[1].data
        print(item[0], "=", item[1].data)
    return hudi_options


def create_checkpoint(num):
    with open(checkpoint_file, "w") as filetowrite:
        filetowrite.write(str(num))


def read_checkpoint():
    if os.path.isfile(checkpoint_file):
        with open(checkpoint_file, "r") as filetoread:
            return int(filetoread.read())
    return None


spark = SparkSession.builder.appName("hudi-testing").getOrCreate()
sc = spark.sparkContext

num = read_checkpoint()
for part, _files in get_data_partitions():
    if len(_files) > 0 and (num is None or part > int(num)):
        df = spark.read.parquet(*_files)
        df.write.format("hudi").options(**get_properties()).mode("append").save(base_path)
        create_checkpoint(part)
