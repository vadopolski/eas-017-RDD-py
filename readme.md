/usr/bin/spark-submit \
--master yarn \
--class TestAppMain \
--deploy-mode cluster \
--verbose \
s3://eas-015/jobs/amr-spark-application.jar s3://eas-015/movies/movies.json s3://eas-015/target/


/usr/bin/spark-submit \
--master yarn \
-- spark.executor.memory 8G \
-- spark.executor.pyspark.memory 8G \

--total-executor-cores 100 \

--spark.dynamicAllocation.enabled true \

-- spark.executor.cores 2 \ 
-- num-executors 4 \

--verbose \
s3://spark-luxoft/python/TestApp.py





-- spark.executor.memory 1G \
executor 100 cores 



50 
-- spark.executor.cores 50 \
-- num-executors 1 \ ~ 1 machine ~ 1 network interfaces ~ limited



50
-- spark.executor.cores 1 \
-- num-executors 50 \ ~ 50 machine ~ 50 network interfaces ~ 

50 





50
-- spark.executor.cores 1 \
-- num-executors 50 \
-- spark.executor.memory 1G \

network 4 10GB = 40GB


50
-- spark.executor.cores 2 \
-- num-executors 25 \
-- spark.executor.memory 2G \

50
-- spark.executor.cores 4 \
-- num-executors 13 \
-- spark.executor.memory 4G \