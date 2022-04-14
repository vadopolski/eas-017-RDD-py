from time import sleep

from pyspark.sql import SparkSession, Window
from pyspark.sql.types import *
from pyspark.sql.functions import *

# SparkSession is the entry point for the HIGH-LEVEL API (DataFrames, Spark SQL)
spark = SparkSession. \
    builder. \
    appName("Joins"). \
    master("local"). \
    getOrCreate()


def df_exercise():
    movies_df = spark.read. \
        format("json"). \
        option("inferSchema", "true"). \
        load("../sources/movies")

    # 1
    # what's wrong with a SinglePartition
    # how to add column with row_num() and count()
    # read.parquet.count use schema

    whole_dataset = Window.partitionBy().orderBy(col("Title").asc_nulls_last())

    single_part_df = movies_df.select(col("Title"), row_number().over(whole_dataset))
    single_part_df.explain()
    # single_part_df.show()

    non_single_part_df = movies_df.select(col("Title"), monotonically_increasing_id())
    non_single_part_df.explain()
    # single_part_df.sample(0.1).show()

    # 2
    # How to read all data from cache?
    # Partial caching - cashing only parts which were calculated by some action. That is the couse that part of data
    # was from cache the other from source.

    partition_of_100_df = spark.range(0, 10000, 1, 100)
    partition_of_100_df.cache()

    # use only one partition, use only one partition FRACTION CACHE 1% - http://localhost:4040/storage/
    # consistence can be uncorrected USE .count to put all data to cache
    # deserialized - as Java object, serialized - as Array[Byte]

    # partition_of_100_df.show(1)

    partition_of_100_df.count()
    partition_of_100_df.show(1)

    # show data on local disk and disk spil
    # InMemoryRelation - load data to cache

    partition_of_100_df.explain()
    # InMemoryTableScn - load data to cache

    # 3 Coalesce vs repartition




if __name__ == '__main__':
    df_exercise()
    sleep(10000)
