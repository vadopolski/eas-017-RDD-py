/usr/bin/spark-submit \
--master yarn \
--class TestAppMain \
--deploy-mode cluster \
--verbose \
s3://eas-015/jobs/amr-spark-application.jar s3://eas-015/movies/movies.json s3://eas-015/target/


/usr/bin/spark-submit \
--master yarn \
--verbose \
s3://spark-luxoft/python/TestApp.py
