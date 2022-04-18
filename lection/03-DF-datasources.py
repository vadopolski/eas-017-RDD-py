from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession. \
    builder. \
    appName("Data Sources"). \
    master("local"). \
    config("spark.jars", "../jars/postgresql-42.2.19.jar"). \
    config("spark.sql.legacy.timeParserPolicy", "LEGACY"). \
    getOrCreate()


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

    cars_df.show()

    cars_df_v2 = spark.read. \
        format("json"). \
        options(mode="failFast", path="../sources/cars", inferSchema="true"). \
        load()
             # /sources/cars
    # 10.1.1.1 node1 -> block1     S3 NETWORK                             -> partition1 -> task1
    # 10.1.1.2 node2 -> block2 -> Spark Driver -> Name Node -> ip adress -> partition2 -> task2
    # 10.1.1.3 node3 -> block2                                           -> parttion3 -> task3

    #


    cars_df.\
        repartition(3). \
        write. \
        format("json"). \
        mode("overwrite"). \
        option("path", "../sources/cars_dupe"). \
        save()


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
        json("../data/cars")  # equivalent to .format(...).option("path",...).load()

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
        csv("../data/stocks")  # same as .option("path", "...").format("csv").load()

    # Parquet = binary data, high compression, low CPU usage, very fast
    # also contains the schema
    # the default data format in Spark

    stocks_df.write.save("../data/stocks_parquet")

    # each row is a value in a DF with a SINGLE column ("value")
    text_df = spark.read.text("../data/lipsum")
    text_df.show()

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
url = "jdbc:postgresql://localhost:5432/eas017"
user = "docker"
password = "docker"


def jdbc_postgres_oracle():
    employees_df = spark.read. \
        format("jdbc"). \
        option("driver", driver). \
        option("url", url). \
        option("user", user). \
        option("password", password). \
        option("dbtable", "public.employees"). \
        load()

    employees_df.show()
# DB/2 connector

def mpp_cassandra_mongo_vertica():
    state_names_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("../sources/statenames")

    state_names_df.show()
# GreenPlum

def queue_kafka():
    state_names_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("../sources/statenames")

    state_names_df.show()

    """
    Exercise: read the movies DF, then write it as
    - tab-separated "CSV"
    - parquet
    - table "public.movies" in the Postgres DB

    Exercise #2: find a way to read the people-1m dataFrame. Then write it as JSON.
    """

if __name__ == '__main__':
    file_system_HDFS_S3_FTP()
