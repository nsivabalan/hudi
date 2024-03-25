#!/bin/bash

export WORKING_DIRECTORY=$(pwd)

usage="
USAGE:
$(basename "$0") [--help] -- Script to run error injection hudi job and validate the metadata or data table.

where:
    --help show this help text
    --base_table_name name of hudi table (DEFAULT - hoodie_table)
    --base_path base path for hudi table (DEFAULT - None)
    --enable_metadata true if metadata enbled else false  (DEFAULT - true)
    --table_type hoodie table type to test (DEFAULT COPY_ON_WRITE)
    --packages Packages for spark command (DEFAULT - org.apache.spark:spark-avro_2.12:3.2.0)
    --jars Jars for spark command (DEFAULT - None)
    --hudi_jar hudi jar full path (DEFAULT - None)
    --disable_compaction disable compaction (DEFAULT - false)
    --deltastreamer_or_datasource run spark job as deltastreamer or SparkDataSourceContinuousIngestTool (DEFAULT - deltastreamer)
    --async_or_inline_compaction  compaction type inline or async - only applicable for MOR (DEFAULT async)
    --async_or_inline_cleaning  cleaning type inline or async (DEFAULT inline)
    --enable_clustering enable clustering (DEFAULT - false)
    --async_or_inline_clustering  clustering type inline or async (DEFAULT async)
    --use_pre_generated_dataset use pre generated dataset. (DEFAULT false)
    --pre_generated_dataset_size data size from already available (small, medium, large), if --use_pre_generated_dataset specified, it will automatically downloaded (DEFAULT small)
    --input_data_path input data path, if --use_pre_generated_dataset specified, it will automatically downloaded and set (DEFAULT WORKING_DIRECTORY/generated_data/{datat_size_3f})
    --download_spark if true it will download the spark 3.2.0 and set SPARK_HOME (DEFAULT false)
    --spark_home to set SPARK_HOME if --download_spark is false (DEFAULT false)
    --properties_file_path custom properties file, other properties related option will be null and void. hoodie.deltastreamer.source.dfs.root will be set by script (DEFAULT None)
    --additional_spark_conf any additional spark command conf (DEFAULT - None)
    --job_timeout_mins job will be killed/timeout after these many minutes (DEFAULT - 60)
    --validate_all_files if need to validate all the file groups (DEFAULT - false)
    --master spark master (DEFAULT - yarn)
    --deploy_mode spark deploy-mode (DEFAULT - client)
    --op HUDI table operation (DEFAULT - UPSERT)
Example:
  1. To run simple test
      sh run_inject_error_test_emr.sh --input_data_path <INPUT_DATASET_PATH> --download_spark true --base_path /home/hadoop/output --hudi_jar /path/to/hudi-utilities-bundle.jar
  2. To run custom properties file
      sh run_inject_error_test_emr.sh --properties_file_path=/path/to/file --additional_spark_conf '--hoodie-conf hoodie.clustering.inline=true'
 "

# - Read directly from s3 ()
# - default clustering is disable

# export JAVA_HOME="/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.312.b07-1.amzn2.0.2.x86_64"
# export SPARK_HOME="/home/ec2-user/spark-3.2.0-bin-hadoop3.2"
export MAIN_RUN_DIR=$WORKING_DIRECTORY/job_runs
export RUN_DIR=$MAIN_RUN_DIR/"`date +"%d-%m-%YT%H-%M-%S"`"
mkdir -p $RUN_DIR
export TABLE_NAME="hoodie_table"
export HUDI_JAR=""
export BASE_PATH=""
export INPUT_DATA_DIR="$WORKING_DIRECTORY/generated_data"
export EVENT_LOG="$RUN_DIR/spark_event_logs"
mkdir -p $EVENT_LOG
export PROPS_DIR="$WORKING_DIRECTORY/sample_props"
export JARS="$WORKING_DIRECTORY/hudi-utilities-bundle.jar"
export TABLE_TYPE="COPY_ON_WRITE"
export PROPERTIES_PATH=""
export LOG_FILE=$RUN_DIR/${TABLE_NAME}_logs.log
export REPORT_FILE=$RUN_DIR/${TABLE_NAME}_report.log
export SPARK_FILE=$RUN_DIR/${TABLE_NAME}_spark_submit.txt
export PACKAGES="org.apache.spark:spark-avro_2.12:3.2.0"
export SPARK_TGZ_FILE="spark-3.2.0-bin-hadoop3.2"
export SPARK_VERISON="spark-3.2.0"
export DISABLE_COMPACTION=false
export ENABLE_CLUSTERING=false
export ENABLE_METADATA=true
export ADDITIONAL_SPARK_CONF=""
export CLUSTERING_TYPE="async"
export COMPACTION_TYPE="async"
export CLEANING_TYPE="inline"
export DOWNLOAD_SPARK=false
export OP="UPSERT"
export DRY_RUN=false
export JOB_TIMEOUT_MINUTES=60
export VALIDATE_ALL_FILES=false
export INPUT_DATA_PATH=""
export DELTASTREAMER_OR_DATASOURCE="deltastreamer"
export MASTER=yarn
export DEPLOY_MODE=client

POSITIONAL=()
while [[ $# -gt 0 ]]
do
key="$1"

case $key in
    --help)
    echo "$usage"
    exit
    ;; 
    --base_table_name)
    TABLE_NAME="$2"
    shift # past argument
    shift # past value
    ;;
    --table_type)
    TABLE_TYPE="$2"
    shift # past argument
    shift # past value
    ;;
    --packages)
    PACKAGES="$2"
    shift # past argument
    shift # past value
    ;;
    --jars)
    JARS="$2,$JARS"
    shift # past argument
    shift # past value
    ;;
    --hudi_jar)
    HUDI_JAR="$2"
    shift # past argument
    shift # past value
    ;;
    --deltastreamer_or_datasource)
    DELTASTREAMER_OR_DATASOURCE=$(echo "$2" | tr '[:upper:]' '[:lower:]')
    shift # past argument
    shift # past value
    ;;
    --disable_compaction)
    DISABLE_COMPACTION="$2"
    shift # past argument
    shift # past value
    ;;
    --enable_clustering)
    ENABLE_CLUSTERING="$2"
    shift # past argument
    shift # past value
    ;;
    --enable_metadata)
    ENABLE_METADATA="$2"
    shift # past argument
    shift # past value
    ;;
    --async_or_inline_clustering)
    CLUSTERING_TYPE=$(echo "$2" | tr '[:upper:]' '[:lower:]')
    shift # past argument
    shift # past value
    ;;
    --async_or_inline_compaction)
    COMPACTION_TYPE=$(echo "$2" | tr '[:upper:]' '[:lower:]')
    shift # past argument
    shift # past value
    ;;
    --async_or_inline_cleaning)
    CLEANING_TYPE=$(echo "$2" | tr '[:upper:]' '[:lower:]')
    shift # past argument
    shift # past value
    ;;
    --job_timeout_mins)
    JOB_TIMEOUT_MINUTES="$2"
    shift # past argument
    shift # past value
    ;;
    --base_path)
    BASE_PATH="$2"
    shift # past argument
    shift # past value
    ;;
    --download_spark)
    DOWNLOAD_SPARK="$2"
    shift # past argument
    shift # past value
    ;;
    --op)
    OP="$2" # INSERT, UPSERT, BULK_INSERT
    shift # past argument
    shift # past value
    ;;
    --properties_file_path)
    PROPERTIES_PATH="$2"
    shift # past argument
    shift # past value
    ;;   
    --spark_home)
    SPARK_HOME="$2"
    shift # past argument
    shift # past value
    ;;  
    --additional_spark_conf)
    ADDITIONAL_SPARK_CONF="$2"
    shift # past argument
    shift # past value
    ;;
    --dry_run)
    DRY_RUN="$2"
    shift # past argument
    shift # past value
    ;;
    --master)
    MASTER="$2"
    shift # past argument
    shift # past value
    ;;
    --deploy_mode)
    DEPLOY_MODE="$2"
    shift # past argument
    shift # past value
    ;;
    --validate_all_files)
    VALIDATE_ALL_FILES="$2"
    shift # past argument
    shift # past value
    ;;
    --input_data_path)
    INPUT_DATA_PATH="$2"
    shift # past argument
    shift # past value
    ;;
    --default)
    DEFAULT=YES
    shift # past argument
    ;;
    *)    # unknown option
    POSITIONAL+=("$1") # save it in an array for later
    echo "Unknown argument provided - '$1'"
    echo "$usage"
    exit 1
    shift # past argument
    ;;
esac
done
set -- "${POSITIONAL[@]}" # restore positional parameters


if [ "$HUDI_JAR" == "" ]; then
    echo "Error - hudi_jar does not exist. Please set --hudi_jar"
    exit 1
fi
if [[ "$BASE_PATH" == "" ]]; then
    echo "Error - `--base_path` is not set. Please set --base_path"
    exit 1
fi

if [[ "$PROPERTIES_PATH" == "" ]]
then
    if $ENABLE_CLUSTERING ;
    then
        if [[ $CLUSTERING_TYPE == "inline" ]]
        then
            ADDITIONAL_SPARK_CONF=$ADDITIONAL_SPARK_CONF" --hoodie-conf hoodie.clustering.inline=true "
        fi

        if [[ "$CLUSTERING_TYPE" == "async" ]]
        then
            ADDITIONAL_SPARK_CONF=$ADDITIONAL_SPARK_CONF" --hoodie-conf hoodie.clustering.async.enabled=true "
        fi
        if  [[ "$CLUSTERING_TYPE" == "inline" ]] && [[ "$OP" == "BULK_INSERT" ]] 
        then
            PROPERTIES_PATH="$PROPS_DIR/inline-clustering-bulk.properties"
        fi
        if  [[ "$CLUSTERING_TYPE" == "inline" ]] && [[ "$OP" != "BULK_INSERT" ]]  
        then
            PROPERTIES_PATH="$PROPS_DIR/inline-clustering.properties"
        fi
        if  [[ "$CLUSTERING_TYPE" == "async" ]] && [[ "$OP" == "BULK_INSERT" ]] 
        then
            PROPERTIES_PATH="$PROPS_DIR/async-clustering-bulk.properties"
        fi
        if  [[ "$CLUSTERING_TYPE" == "async" ]] && [[ "$OP" != "BULK_INSERT" ]] 
        then
            PROPERTIES_PATH="$PROPS_DIR/async-clustering-bulk.properties"
        fi
    else
        PROPERTIES_PATH="$PROPS_DIR/test.properties"
    fi
    if  [[ "$COMPACTION_TYPE" == "inline" ]] && [[ "$TABLE_TYPE" == "COPY_ON_WRITE" ]]
    then
        echo "Error - Compaction for COPY_ON_WRITE is not available."
        exit 1
    fi

    cp $PROPERTIES_PATH $RUN_DIR/run.properties
    PROPERTIES_PATH=$RUN_DIR/run.properties

    if  [[ "$COMPACTION_TYPE" == "inline" ]] && [[ "$TABLE_TYPE" == "MERGE_ON_READ" ]]
    then
        echo "
        hoodie.compact.inline=true" >> $PROPERTIES_PATH
    fi

    if  [[ "$COMPACTION_TYPE" == "async" ]] && [[ "$TABLE_TYPE" == "MERGE_ON_READ" ]]
    then
        echo "" >> $PROPERTIES_PATH
    fi

    if  [[ "$CLEANING_TYPE" == "inline" ]]
    then
        echo "
        hoodie.clean.async=false" >> $PROPERTIES_PATH
    fi

    if  [[ "$CLEANING_TYPE" == "async" ]]
    then
        echo "
        hoodie.clean.async=true" >> $PROPERTIES_PATH
    fi

    if  $ENABLE_METADATA
    then
        echo "
        hoodie.metadata.enable=true" >> $PROPERTIES_PATH
        echo "
        hoodie.write.concurrency.mode=optimistic_concurrency_control" >> $PROPERTIES_PATH
        echo "
        hoodie.cleaner.policy.failed.writes=LAZY" >> $PROPERTIES_PATH
        echo "
        hoodie.write.lock.provider=org.apache.hudi.client.transaction.lock.InProcessLockProvider" >> $PROPERTIES_PATH
    else
        echo "
        hoodie.metadata.enable=false" >> $PROPERTIES_PATH
    fi

else
    cp $PROPERTIES_PATH $RUN_DIR/run.properties
    PROPERTIES_PATH=$RUN_DIR/run.properties

fi

if [[ "$INPUT_DATA_PATH" == "" ]]; then
    echo "Error - `--input_data_path` is not set. Please set --input_data_path"
    exit 1
fi

echo "
hoodie.deltastreamer.source.dfs.root=$INPUT_DATA_PATH" >> $PROPERTIES_PATH
echo "
benchmark.input.source.path=$INPUT_DATA_PATH" >> $PROPERTIES_PATH
echo "
hoodie.table.name=$TABLE_NAME" >> $PROPERTIES_PATH

echo "
hoodie.datasource.write.operation=$OP" >> $PROPERTIES_PATH
echo "
hoodie.datasource.write.table.type=$TABLE_TYPE" >> $PROPERTIES_PATH

if $DISABLE_COMPACTION 
 then
    ADDITIONAL_SPARK_CONF=$ADDITIONAL_SPARK_CONF" --disable-compaction "
fi

if $DOWNLOAD_SPARK
 then
    curl https://archive.apache.org/dist/spark/$SPARK_VERISON/$SPARK_TGZ_FILE.tgz -o $SPARK_TGZ_FILE.tgz
    tar -xf $SPARK_TGZ_FILE.tgz
    SPARK_HOME=$SPARK_TGZ_FILE
    curl https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.48/aws-java-sdk-bundle-1.12.48.jar --output $SPARK_HOME/jars/aws-java-sdk-bundle-1.12.48.jar
    chmod 644 $SPARK_HOME/jars/aws-java-sdk-bundle-1.12.48.jar
    curl https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.1/hadoop-aws-3.3.1.jar -o $SPARK_HOME/jars/hadoop-aws-3.3.1.jar                                      
    chmod 644 $SPARK_HOME/jars/hadoop-aws-3.3.1.jar

fi

if [[ $SPARK_HOME == "" ]];
 then
    echo "Error - Please set SPARK_HOME or pass `--spark_home /some-spark-dir`"
    exit 1
fi

upload_logs_to_s3 () {
  echo "Uploading logs to s3 ${S3_LOG_URI}"
  python3 check_test_status.py --log_file $LOG_FILE >$REPORT_FILE 2>&1
  aws s3 sync $RUN_DIR $S3_LOG_URI
}

exit_if_validation_failed () {
    if grep -q "org.apache.hudi.exception.HoodieValidationException" "$LOG_FILE"; then
        upload_logs_to_s3
        exit 1
    fi
}

# select partition, count(distinct key) from tbl GROUP BY 1
# select partition, count(distinct key) from tbl group by partition
# select count(distinct partition, key) from tbl

exit_if_no_data_to_process () {
    if grep -q "No new data, source checkpoint has not changed. Nothing to commit." "$LOG_FILE"; then
        upload_logs_to_s3
        echo "No new data, source checkpoint has not changed. Nothing to commit."
        exit 0
    fi
    if grep -q "All batches have been consumed. Exiting." "$LOG_FILE"; then
        upload_logs_to_s3
        echo "All batches have been consumed."
        exit 0
    fi
}


SPARK_COMMAND="$SPARK_HOME/bin/spark-submit \
      --master $MASTER --deploy-mode $DEPLOY_MODE \
      --driver-memory 10g --executor-memory 11g --num-executors 4 --executor-cores 3 \
      --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
      --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.DefaultAWSCredentialsProviderChain \
      --conf spark.sql.catalogImplementation=hive \
      --conf spark.driver.maxResultSize=1g \
      --conf spark.ui.port=6681 \
      --conf spark.ui.proxyBase="" \
      --conf spark.eventLog.enabled=true \
      --conf spark.eventLog.dir=hdfs:///var/log/spark/apps \
      --conf spark.hadoop.yarn.timeline-service.enabled=false \
      --packages $PACKAGES \
      --jars $JARS"

DELTASTREAM_JOB="$SPARK_COMMAND \
      --class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer \
      $HUDI_JAR \
      --props file:$PROPERTIES_PATH \
      --continuous $ADDITIONAL_SPARK_CONF \
      --source-class BenchmarkDataSource \
      --source-ordering-field ts \
      --target-base-path $BASE_PATH/$TABLE_NAME \
      --target-table $TABLE_NAME \
      --table-type $TABLE_TYPE \
      --op $OP
"

PYSPARK_DATASOURCE="$SPARK_COMMAND,$HUDI_JAR spark_datasource.py"

SPARK_DATASOURCE="$SPARK_COMMAND \
      --class org.apache.hudi.integ.testsuite.SparkDataSourceContinuousIngestTool \
        $HUDI_JAR \
      --source-path $INPUT_DATA_PATH   \
      --checkpoint-file-path $RUN_DIR/checkpoint  \
      --base-path $BASE_PATH/$TABLE_NAME \
      --props file:$PROPERTIES_PATH
"

LATEST_FILES_META_VALIDATOR="$SPARK_COMMAND \
      --class org.apache.hudi.utilities.HoodieMetadataTableValidator \
        $HUDI_JAR \
      --base-path $BASE_PATH/$TABLE_NAME \
      --validate-latest-file-slices \
      --validate-latest-base-files
"

echo $LATEST_FILES_META_VALIDATOR

ALL_FILES_META_VALIDATOR="$SPARK_COMMAND \
      --class org.apache.hudi.utilities.HoodieMetadataTableValidator \
        $HUDI_JAR \
      --base-path $BASE_PATH/$TABLE_NAME \
      --validate-all-file-groups
"

DATA_TABLE_VALIDATOR="$SPARK_COMMAND \
      --class org.apache.hudi.utilities.HoodieDataTableValidator \
        $HUDI_JAR \
      --base-path $BASE_PATH/$TABLE_NAME
"

JOB_COMMAND=""
    if  [[ "$DELTASTREAMER_OR_DATASOURCE" == "deltastreamer" ]]
    then
        JOB_COMMAND=$DELTASTREAM_JOB
    elif [[ "$DELTASTREAMER_OR_DATASOURCE" == "datasource" ]]
    then
        JOB_COMMAND=$SPARK_DATASOURCE
    else
        echo "Error: invalid value for --deltastreamer_or_datasource, valid values 'deltastreamer', 'datasource'"
        exit 1
    fi

if $DRY_RUN
 then
    echo "\n************************************************************************************************************************\n\n"
    echo "Properties file - $PROPERTIES_PATH\n"
    
    cat $PROPERTIES_PATH
    echo "\n************************************************************************************************************************\n\n"
    echo "Spark Submit Command-"
    echo "\n"$JOB_COMMAND
    exit 0
fi

echo "Logs are available at $LOG_FILE"

SECONDS=0
TOTAL=$(( 60*$JOB_TIMEOUT_MINUTES ))
echo "Job will be timeout after $TOTAL seconds OR $JOB_TIMEOUT_MINUTES minutes"

for ((i = 0; i<2000; i++)); do
    echo "Start deltastreamer run $i ..." >>$LOG_FILE 2>&1
    upload_logs_to_s3
    DELTA_TIMEOUT=$(( $TOTAL-$SECONDS ))

    if [[ $DELTA_TIMEOUT -lt 0 ]]
    then
        break
    fi

    echo "Spark Submit Command-" >>$LOG_FILE 2>&1
    echo "\n"$JOB_COMMAND >>$LOG_FILE 2>&1
    timeout $DELTA_TIMEOUT $JOB_COMMAND >>$LOG_FILE 2>&1

    if  $ENABLE_METADATA
    then
        echo "Validate latest file slides and base files run $i ..." >>$LOG_FILE 2>&1
        $LATEST_FILES_META_VALIDATOR >>$LOG_FILE 2>&1
        
        if $VALIDATE_ALL_FILES
        then
            echo "Validate all file groups run $i ..." >>$LOG_FILE 2>&1
            $ALL_FILES_META_VALIDATOR >>$LOG_FILE 2>&1
            
            VALIDATION_RESULT2=$?
            python3 check_test_status.py --log_file $LOG_FILE --run_id $i --all_files true
            exit_if_validation_failed

        else
            python3 check_test_status.py --log_file $LOG_FILE --run_id $i 
            exit_if_validation_failed
        fi
    else
        echo "Validate datatable for all file groups $i ..." >>$LOG_FILE 2>&1
        $DATA_TABLE_VALIDATOR >>$LOG_FILE 2>&1
    fi
    exit_if_no_data_to_process
done

python3 check_test_status.py --log_file $LOG_FILE

upload_logs_to_s3
