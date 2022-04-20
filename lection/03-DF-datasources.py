from time import sleep

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, from_json, date_format, to_timestamp
from pyspark.sql.types import *
from pyspark.sql import functions as F
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'


spark = SparkSession. \
    builder. \
    appName("Data Sources"). \
    master("local"). \
    config("spark.jars", "../jars/postgresql-42.2.19.jar"). \
    getOrCreate()


spark.conf.set("spark.sql.shuffle.partitions", 14)

# config("spark.python.worker.memory", "8g"). \
#     config("spark.driver.memory", "8g"). \
#     config("spark.executor.memory", "8g"). \
#  \


def file_system_HDFS_S3_FTP():
    # read a DF in json format
    cars_df = spark.read. \
        format("json"). \
        option("inferSchema", "true"). \
        option("mode", "failFast"). \
        option("path", "../sources/cars"). \
        load()

    # HDFS
    # option("path", "hdfs://nn1home:8020/sources/cars"). \

    # FTP
    # option("path", "ftp://user:pwd/192.168.1.5/sources/cars"). \

    # S3
    # option("path", s3://bucket-name/sources/cars)

    # cars_df.show()

    # cars_df_v2 = spark.read. \
    #     format("json"). \
    #     options(mode="failFast", path="../sources/cars", inferSchema="true"). \
    #     load()
             # /sources/cars
    # 10.1.1.1 node1 -> block1     S3 NETWORK                             -> partition1 -> task1
    # 10.1.1.2 node2 -> block2 -> Spark Driver -> Name Node -> ip adress -> partition2 -> task2
    # 10.1.1.3 node3 -> block2                                           -> parttion3 -> task3

    #


    # cars_df.\
    #     repartition(3). \
    #     write. \
    #     mode("overwrite"). \
    #     option("compression", "snappy"). \
    #     parquet("../sources/parquet"). \
    #     save()


    # spark.sql("create schema example")
    #
    # cars_df.\
    #     write. \
    #     saveAsTable("example.cars_table")
    #
    # data_frame = spark.sql("select * from example.cars_table")

    # cars_df. \
    #     repartition(cars_df.Year). \
    #     groupBy(cars_df.Year). \
    #     write. \
    #     partitionBy(cars_df.Year). \
    #     json("../sources/json"). \
    #     save()


    # spark.sql("drop table example.cars_table")

    # data_frame.show()

    # OZU           HDFS data node
    # partition1 -> block1 -> node1 v
    # partition2 -> block2 -> node2 v
    # partition3 -> block3 -> node3 -> error
    # spark.yarn.max.executor.failures

    # 3 ways
    # 1 -> transaction -> two phase commit
    # 2 -> exception -> part recored
    # 3 -> rettry ->



    # Writing modes: overwrite, append, ignore, errorIfExists
    # applicable to all file formats

    # difference between coalesce and repartition - slide picture
    # difference between append, overwrite, .... - slide pict
    # show the compression

    cars_schema = StructType([
        StructField("Name", StringType()),
        StructField("Acceleration", DoubleType()),
        StructField("Cylinders", LongType()),
        StructField("Displacement", DoubleType()),
        StructField("Horsepower", LongType()),
        StructField("Miles_per_Gallon", DoubleType()),
        StructField("Origin", StringType()),
        StructField("Weight_in_lbs", LongType()),
        StructField("Year", DateType()),
    ])

    # JSON flags
    cars_df_v3 = spark.read. \
        schema(cars_schema). \
        option("dateFormat", "YYYY-MM-dd"). \
        option("allowSingleQuotes", "true"). \
        option("compression", "uncompressed"). \
        json("../sources/cars")  # equivalent to .format(...).option("path",...).load()

    stocks_schema = StructType([
        StructField("company", StringType()),
        StructField("date", DateType()),
        StructField("price", DoubleType())
    ])

    # CSV flags
    stocks_df = spark.read. \
        schema(stocks_schema). \
        option("dateFormat", "MMM d YYYY"). \
        option("header", "false"). \
        option("sep", ","). \
        option("nullValue", ""). \
        csv("../sources/stocks")  # same as .option("path", "...").format("csv").load()


    cars_df.\
        select(col("Year")).\
        distinct().\
        show()

    # How to work map reduce with sorting
    grouped_cars_df = cars_df.sort(col("Year"))
    # |||||||||||||   1970 - task1 - |||||
    #                 1971 - task2 - |||||
    #                 1972 - task3 - |||||
    #                 1973 - task4 -


#  1970 - 1975, 1976 - 1980, 1981 - 1985
# 1 -> 100
#     150 000 000 -> 20 ->

# 1,5,6, ..... 50, 51,52,53, ....100





    # grouped_cars_df2 = cars_df.repartition(col("Year"))
    #                                                    1 ~ 200 =   1, 150, 13, 99, ....
    # |||||||||||||   - 1972-01-01 => hash  123123123123123 => long 200 partition_1 ~ task1 => ||| 1972-01-01 -> file1 folder1
    #                                                                   partition_2 ~ task2 =>
    #                                                                   partition_3 ~ task3 => || 1976-01-01 -> file2 folder2
    #                                                                   partition_4 ~ task4 =>
    #                                                                   partition_5 ~ task5 => |||| 1978-01-01 1975-01-01 -> file3 folder3
    #                                                   .....
    #                                                                   partition200 ~ task200 => |||| 1978-01-01 1975-01-01 -> file3 folder3


    # grouped_cars_df3 = cars_df.repartition(4) => //
    # grouped_cars_df3 = cars_df.coalesce(1) => OOM

    # |||||||||||||   |||||| - task1 -
    #                 |||||| - task2
    #                 |||||| - task3
    #                 |||||| - task4

    grouped_cars_df.explain(True)


    grouped_cars_df.\
        write. \
        mode("overwrite"). \
        json("../sources/json")

 #    partitionBy("Year"). \
 # \
        # Parquet = binary data, high compression, low CPU usage, very fast
    # also contains the schema
    # the default data format in Spark

    # stocks_df.write.save("../sources/stocks_parquet")

    # each row is a value in a DF with a SINGLE column ("value")
    # text_df = spark.read.text("../data/lipsum")
    # text_df.show()

    # !!!!!!!!!!!!! DIFFERENCE between saveAsTable() and write


def data_formats_json_avro_parquet():
    state_names_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("../sources/statenames")

    state_names_df.show()
    state_names_df.printSchema()

    state_names_df \
        .coalesce(1) \
        .write \
        .mode("overwrite") \
        .parquet("../target/statenames_parquet")


# reading data from external JDBC (Postgres)
driver = "org.postgresql.Driver"
url = "jdbc:postgresql://localhost:5432/spark"
user = "docker"
password = "docker"


def jdbc_postgres_oracle():
    DBPARAMS = {
        "user": user,
        "password": password,
        "driver": driver
    }

    employees = "public.employees"
    employees_pruned = """(select e.first_name, e.last_name, e.hire_date from public.employees e where e.gender = 'F') as new_emp"""


    # 10101        99999
    # 10102        99998
    # 10103        10103





    # df = spark.\
    #     read.\
    #     jdbc(url=url, table=employees, properties=DBPARAMS)

    # print("GET NUM PARTITIONS")
    # print(df.rdd.getNumPartitions())

    # df.printSchema()
    # df.agg(F.max(F.col("emp_no")), F.min(F.col("emp_no"))).show()


    # df = spark.read.jdbc(url=url, table="public.employees", properties=DBPARAMS,
    #                      column="emp_no", lowerBound = 10010, upperBound = 499990, numPartitions = 10)

    # lowerBound = 10010
    # upperBound = 499990
    #
    # ex1 => part1 => select * from public.employees e where e.emp_num > x and e.emp_num
    # ex2 => part2 =

    pred = ["gender = 'F'", "gender = 'M'", "gender = 'M'"]
    # be carefully with borders
    pred2 = ["emp_no > 10010 and emp_no <= 50000", "emp_no >= 50000 and emp_no <= 100000"]

    df = spark.read.jdbc(url=url, table="public.employees", properties=DBPARAMS, predicates =pred)
    df.show()


    # lowerBound = 10010,
    # upperBound = 499990,
    # numPartitions = 20,

    # Killer joins => optimised UDF

    # print("GET NUM PARTITIONS")
    # print(df.rdd.getNumPartitions())
    #
    # df.show()



    employees_df = spark.read. \
        format("jdbc"). \
        option("driver", driver). \
        option("url", url). \
        option("user", user). \
        option("password", password). \
        option("dbtable", "public.employees"). \
        load()


    # department_df = spark.read (dept_no, dept_name) // 200
    #
    # employees_df. \
    #     groupBy("dept_no"). \
    #     count(). \
    #     join(department_df, col("dept_no") = col("dept_no"),  "inner")

    # Solution1 UDF
    #



    print("GET NUM PARTITIONS")
    print(employees_df.rdd.getNumPartitions())


    employees_df.show()

    employees_df.write.bucketBy(10, "emp_no").sortBy("emp_no").mode("overwrite").saveAsTable("employee_bucketed")
    # employees_df.write.mode("overwrite").save() Parquet


    # do several joins => sortmerge join
    # bucketing + sorting =


# DB/2 connector

def mpp_cassandra_mongo_vertica():
    state_names_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("../sources/statenames")

    state_names_df.show()
# GreenPlum

def queue_kafka():
    schema = StructType([
        StructField("timestamp", StringType()),
        StructField("page", StringType())
    ])


    # source_batch_df = spark.read\
    #     .format("kafka")\
    #     .option("kafka.bootstrap.servers", "localhost:29092")\
    #     .option("subscribe", "input")\
    #     .load()
    #
    # print(source_batch_df.isStreaming)
    #
    # source_batch_df.show()


    source_streaming_df = spark.readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", "localhost:29092")\
        .option("subscribe", "input")\
        .load()

    print(source_streaming_df.isStreaming)

    # source_streaming_df.writeStream.foreachBatch(lambda b, id: b.show())

    # 3 sec => 1000 => batch1
    # 3 sec => 2000 => batch2
    # 3 sec => 2000 => batch3

    typed_source_streaming_df = source_streaming_df.\
        select(expr("cast(value as string) as actualValue")).\
        select(from_json(col("actualValue"), schema).alias("page")).\
        selectExpr("page.timestamp as timestamp", "page.page as page").\
        select(date_format(to_timestamp(col("timestamp"), "dd-MM-yyyy HH:mm:ss:SSS"), "HH:mm:ss:SSS").alias("time"),col("page")
      )

    source_streaming_df.\
        writeStream.\
        outputMode("append").\
        foreachBatch(lambda b, l: b.show).\
        trigger(processingTime='3 seconds').\
        start().\
        awaitTermination()


    """
    Exercise: read the movies DF, then write it as
    - tab-separated "CSV"
    - parquet
    - table "public.movies" in the Postgres DB

    Exercise #2: find a way to read the people-1m dataFrame. Then write it as JSON.
    """

if __name__ == '__main__':
    queue_kafka()
    sleep(100000)
