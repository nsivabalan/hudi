#!/bin/bash

export WORKING_DIRECTORY=$(pwd)

usage="
USAGE:
$(basename "$0") [--help] -- Script to run error injection hudi job async validation on metadata or data table.

where:
    --help show this help text
    --base_table_name name of hudi table (DEFAULT - hoodie_table)
    --base_path base path for hudi table (DEFAULT - None)
    --enable_metadata true if metadata enbled else false  (DEFAULT - true)
    --packages Packages for spark command (DEFAULT - org.apache.spark:spark-avro_2.12:3.2.0)
    --jars Jars for spark command (DEFAULT - None)
    --hudi_jar hudi jar full path (DEFAULT - None)
    --spark_home to set SPARK_HOME if --download_spark is false (DEFAULT false)
    --validate_all_files if need to validate all the file groups (DEFAULT - false)
    --master spark master (DEFAULT - yarn)
    --deploy_mode spark deploy-mode (DEFAULT - client)
    --is_jenkins if this is jebkins job true else false (DEFAULT - false)
Example:
  To run simple test
      sh run_inject_error_validation.sh  --spark_home /spark_home --base_path /home/hadoop/output --hudi_jar /path/to/hudi-utilities-bundle.jar
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
export INPUT_DATA_SIZE="small"
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
export USE_DATA_FROM_S3=false
export DOWNLOAD_SPARK=false
export OP="UPSERT"
export DRY_RUN=false
export JOB_TIMEOUT_MINUTES=60
export VALIDATE_ALL_FILES=false
export INPUT_DATA_PATH=""
export DELTASTREAMER_OR_DATASOURCE="deltastreamer"
export MASTER=yarn
export DEPLOY_MODE=client
export IS_JENKINS=false

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
    --enable_metadata)
    ENABLE_METADATA="$2"
    shift # past argument
    shift # past value
    ;;
    --base_path)
    BASE_PATH="$2"
    shift # past argument
    shift # past value
    ;;
    --spark_home)
    SPARK_HOME="$2"
    shift # past argument
    shift # past value
    ;;  
    --master)
    MASTER="$2"
    shift # past argument
    shift # past value
    ;;
    --is_jenkins)
    IS_JENKINS="$2"
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


if [[ $SPARK_HOME == "" ]];
 then
    echo "Error - Please set SPARK_HOME or pass `--spark_home /some-spark-dir`"
    exit 1
fi

upload_logs_to_s3 () {
   
    if $IS_JENKINS
    then 
        echo "Uploading logs to s3 ${S3_LOG_URI}"
        python3 check_test_status.py --log_file $LOG_FILE >$REPORT_FILE 2>&1
        aws s3 sync $RUN_DIR $S3_LOG_URI
    fi
}

exit_if_validation_failed () {
    if grep -q "org.apache.hudi.exception.HoodieValidationException" "$LOG_FILE"; then
        upload_logs_to_s3
        process_id=$(pgrep -f ".*run_inject_error_test_emr.*")
        kill "$process_id"
        process_id=$(pgrep -f ".*spark-submit.*")
        kill "$process_id"
        exit 1
    fi
}

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
      --driver-memory 4g --executor-memory 4g --num-executors 4 --executor-cores 2 \
      --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
      --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.DefaultAWSCredentialsProviderChain \
      --conf spark.sql.catalogImplementation=hive \
      --conf spark.driver.maxResultSize=1g \
      --conf spark.ui.port=6681 \
      --conf spark.eventLog.enabled=true \
      --conf spark.eventLog.dir=file:$EVENT_LOG \
      --packages $PACKAGES \
      --jars $JARS"

LATEST_FILES_META_VALIDATOR="$SPARK_COMMAND \
      --class org.apache.hudi.utilities.HoodieMetadataTableValidator \
        $HUDI_JAR \
      --base-path $BASE_PATH/$TABLE_NAME \
      --validate-latest-file-slices \
      --validate-latest-base-files \
      --ignore-failed
"

echo $LATEST_FILES_META_VALIDATOR

ALL_FILES_META_VALIDATOR="$SPARK_COMMAND \
      --class org.apache.hudi.utilities.HoodieMetadataTableValidator \
        $HUDI_JAR \
      --base-path $BASE_PATH/$TABLE_NAME \
      --validate-all-file-groups \
      --ignore-failed
"

DATA_TABLE_VALIDATOR="$SPARK_COMMAND \
      --class org.apache.hudi.utilities.HoodieDataTableValidator \
        $HUDI_JAR \
      --base-path $BASE_PATH/$TABLE_NAME
"

echo "Logs are available at $LOG_FILE"


for ((i = 0; i<2000; i++)); do
    sleep 120
    upload_logs_to_s3
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