echo "calling tiger.sh"

spark-submit  --master "local[*]"  --num-executors 1  --executor-cores 2  --conf spark.cores.max=2  --conf spark.yarn.executor.memoryOverhead=1G  --driver-memory 3G  --executor-memory 2G  "<JAR_FILE_PATH>" "<DATA_FOLDER_PATH>" "<KEYS_FILE_PATH>" "<RESULTS_FOLDER_PATH>"
