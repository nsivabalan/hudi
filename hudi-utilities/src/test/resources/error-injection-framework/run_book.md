

# Error Injection Framework

## Sample command

```bash
nohup bash run_inject_error_test_emr.sh --hudi_jar /home/hadoop/hudi-utilities-bundle_2.12-0.14.0.jar \
--jars PATH_TO_HUDI_BENCHMARKS_JAR \
--input_data_path <INPUT_DATA_LOCATION> \
--base_path <BASE_PATH_FOR_HUDI_TABLE> \
--async_or_inline_clustering inline --async_or_inline_compaction async --async_or_inline_cleaning async \
--disable_compaction false --enable_clustering false --enable_metadata true --op UPSERT \
--download_spark false --master yarn --deploy_mode client --table_type COPY_ON_WRITE --job_timeout_mins 800 &
```

Expected format for INPUT_DATA_LOCATION

Root directory is expected to have sub-folders for each batch of ingestion. Each sub-folder is expected to have 
actual data files to be ingested. One folder will be consumed per batch of ingestion. 

INPUT_DATA_LOCATION/
   0/
      parquet files
   1/
      parquet files 
   2/ 
      parquet files

There are more arguments for run_inject_error_test_emr.sh which can be found by looking at the bash script.

Before running the script, ensure you apply below patch to your branch of interest. This patch will inject failures
at different points in time across write lifecyle w/ diff probabilities.
https://github.com/yihua/hudi/commit/4aea8e830a57621aa7ef2f5f1eb1ebe66beef024

So, the framework will run deltastreamer repeatedly (will restart automatically if job failed due to errors) until we 
exhaust entire input dataset or until the job timeout is hit.  