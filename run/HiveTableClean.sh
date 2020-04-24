#!/bin/bash
source /etc/profile
echo "submit spark job"
spark-submit --master yarn-cluster \
--conf spark.storage.memoryFraction=0.1 \
--executor-cores 2 \
--num-executors 2 \
--executor-memory 2g \
--driver-memory 2g \
--class com.xkj.mlrc.clean.table.HiveTableClean \
data-manager.jar \
-avoidSuffix .jar,.xml \
-expire 3





rc=$?
if [[ $rc != 0 ]]; then
    echo "`date "+%Y-%m-%d %H:%M:%S"` Spark job run failed......"
    exit 1
else
    echo "`date "+%Y-%m-%d %H:%M:%S"` Spark job run successfully......."
fi
