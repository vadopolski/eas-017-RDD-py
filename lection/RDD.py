from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession. \
    builder. \
    appName("RDD"). \
    master("local"). \
    getOrCreate()

sc = spark.sparkContext


def split_row(row):
    return row.split(",")


def rdd_creation():
    # 1. HOW TO CREATE RDD
    # we can build RDDs out of local collections
    numbers = range(1, 1000000)
    numbers_rdd = sc.parallelize(numbers, 4)
    # print(numbers_rdd.collect())

    # read a file in parallel
    stocks_rdd_v2 = sc.textFile("../sources/stocks/aapl.csv"). \
        map(split_row). \
        filter(lambda tokens: float(tokens[2]) > 15)
    # print(stocks_rdd_v2.collect())

    # read from a DF
    stocks_df = spark.read.csv("../sources/stocks"). \
        withColumnRenamed("_c0", "company"). \
        withColumnRenamed("_c1", "date"). \
        withColumnRenamed("_c2", "price")

    stocks_rdd_v3 = stocks_df.rdd  # an RDD of all the rows in the DF
    prices_rdd = stocks_rdd_v3.map(lambda row: row.price)
    # print(prices_rdd.collect())


    # RDD to DF
    # condition: the RDD must contain Spark Rows (data structures conforming to a schema)
    stocks_df_v2 = spark.createDataFrame(stocks_rdd_v3)
    # print(stocks_df_v2.collect())


    """
    Use cases for RDDs
    - the computations that cannot work on DFs/Spark SQL API
    - very custom perf optimizations
    """

    # RDD transformations
    # map, filter, flatMap

    # distinct
    company_names_rdd = stocks_rdd_v3\
        .map(lambda row: row.company)\
        .distinct()
    print(company_names_rdd.collect())

    # counting
    total_entries = stocks_rdd_v3.count()  # action - the RDD must be evaluated

    # min and max
    aapl_stocks_rdd = stocks_rdd_v3\
        .filter(lambda row: row.company == "AAPL")\
        .map(lambda row: row.price)
    max_aapl = aapl_stocks_rdd.max()

    # reduce
    sum_prices = aapl_stocks_rdd\
        .reduce(lambda x, y: x + y)  # can use ANY Python function here

    # grouping
    grouped_stocks_rdd = stocks_rdd_v3\
        .groupBy(lambda row: row.company)  # can use ANY grouping criterion as a Python function
    # grouping is expensive - involves a shuffle

    # partitioning
    repartitioned_stocks_rdd = stocks_rdd_v3\
        .repartition(30)  # involves a shuffle

"""
Exercises
    1. Read the movies dataset as an RDD
    2. Show the distinct genres as an RDD
    3. Print all the movies in the Drama genre with IMDB rating > 6
"""


if __name__ == '__main__':
    rdd_creation()
