from time import sleep

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

# DataFrame
spark = SparkSession. \
    builder. \
    appName("RDD"). \
    master("local"). \
    getOrCreate()
# executors_num
# memory_per_ex
# cores_per_execut
# s = executors_num * cores_per_execut = 400 slotes
#  20 block => 20 slotes ~ 95%
#

# RDD
sc = spark.sparkContext


def split_row(row):
    return row.split(",")

def rdd_creation():
    # 1. HOW TO CREATE RDD
    # we can build RDDs out of local collections
    numbers = range(1, 1000000)
    numbers_rdd = sc.parallelize(numbers, 4)
    numbers_rdd_2 = numbers_rdd.map(lambda x: x + 1)
    # Dependency: numbers_rdd => numbers_rdd_2
    # Linage: partition: block => numbers_rdd => numbers_rdd_2
    # print(numbers_rdd_2.collect())

    # read a file in parallel
    stocks_rdd_v2 = sc.textFile("../sources/stocks/aapl.csv", 4). \
        map(lambda row: row.split(",")). \
        filter(lambda tokens: float(tokens[2]) > 15)
    # print(stocks_rdd_v2.collect())
    # protected def getPartitions: Array[Partition]

    # read from a DF
    stocks_df = spark.read.csv("../sources/stocks"). \
        withColumnRenamed("_c0", "company"). \
        withColumnRenamed("_c1", "date"). \
        withColumnRenamed("_c2", "price")

    stocks_rdd_v3 = stocks_df.rdd  # an RDD of all the rows in the DF
    # Row => InternalRow
    prices_rdd = stocks_rdd_v3.map(lambda row: row.price)
    prices_rdd.toDebugString()
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
    company_names_rdd = stocks_rdd_v3 \
        .map(lambda row: row.company) \
        .distinct()
    # print(company_names_rdd.collect())

    # counting
    total_entries = stocks_rdd_v3.count()  # action - the RDD must be evaluated
    # print(total_entries)

    # min and max
    aapl_stocks_rdd = stocks_rdd_v3 \
        .filter(lambda row: row.company == "AAPL") \
        .map(lambda row: float(row.price))
    max_aapl = aapl_stocks_rdd.max()
    # print(max_aapl)

    # reduce
    sum_prices = aapl_stocks_rdd \
        .reduce(lambda x, y: x + y)  # can use ANY Python function here  1,2,3,4 => 1+2 = 3 + 3 = 6 + 4
    print(sum_prices)

    # grouping
    grouped_stocks_rdd = stocks_rdd_v3 \
        .groupBy(lambda row: row.company)  # can use ANY grouping criterion as a Python function
    # grouping is expensive - involves a shuffle
    # print(grouped_stocks_rdd.collect())

    # partitioning
    repartitioned_stocks_rdd = stocks_rdd_v3 \
        .coalesce(2)
        # .repartition(30)  # involves a shuffle
          # involves a shuffle
#  .repartition(5) 100
#  part1 => |||||| 20           20 2  =>
#  part2 => |||||||||||||| 40   20 2  => |||||||||||||| 40 + |||||| 20 = 60
#  part3 => ||||| 10            20 2
#  part4 => |||||||||| 30       20 2  => |||||||||| 30 + ||||| 10 = 40
#  part5 =>                     20 2


"""
Exercises
    1. Read the movies dataset as an RDD
    2. Show the distinct genres as an RDD
    3. Print all the movies in the Drama genre with IMDB rating > 6
"""

    # movies_df = spark.read.json("../sources/movies")
    # movies_rdd = movies_df.rdd


def rdd_saving():
    # r = [1, 2, 3, 4, 5, 6, 7, 8]
    # ints = sc.parallelize(r, 4).repartition(15)


    # ints = sc.parallelize(r).coalesce(1)
    # ints.coalesce(1)\
    #     .saveAsTextFile("../ints")

    cachedInts = sc.textFile("../ints", 4)\
        .map(lambda x: int(x))\
        .persist(StorageLevel.MEMORY_AND_DISK)
        # .cache()
    cachedInts.count()

    # cachedInts.map(lambda x: x + 1).cache()

    # cachedInts.unpersist()

    #  very important to count() after cashing
    # cachedInts.first()
    # cachedInts.count()

    # cachedInts.map(lambda x: x +1).collect()
    # cachedInts.reduce(lambda x, y: x + y)

    doubles = cachedInts.map(lambda x: x * 2)
    print("== Doubles")
    for el in doubles.collect():
        print(el)


    even = cachedInts.filter(lambda x: x % 2 == 0)
    print("== Even")
    for el in even.collect():
        print(el)

    even.setName("Even numbers")
    print("Name is " + even.name() + " id is " + str(even.id()))

    plan = even.toDebugString

    print(plan)

    # cachedInts.unpersist()
    print("Multiply all numbers => " + str(even.reduce(lambda a, b: a * b)))
    print(even.toDebugString)


def group_join():
    data = [("Ivan", 240), ("Petr", 39), ("Elena", 290), ("Elena", 300)]
    codeRows = sc.parallelize(data)

    print("== Deduplicated")
     # Let's calculate sum of code lines by developer

    reduced = codeRows.reduceByKey(lambda x, y: x + y)
    print(reduced.collect())
    deduplicated = codeRows.reduceByKey(lambda x, y: x if (x > y) else y)
    # for el in deduplicated.collect():
    #     print(el)

    # print()
    # print("== Folded")
    # folded = codeRows.foldByKey(1000, lambda x, y: x + y)
    #
    # for el in folded.collect():
    #     print(el)

    print()
    print("== Aggregated")
    aggregated = codeRows.aggregateByKey(500, lambda x, y: x + y, lambda x, y: x + y)
    for el in aggregated.collect():
        print(el)
    #     part1 (k1:2, k2:2, k3:2, k1:2) shufle => (k1:2, k1:2, k1:2) => k1:6
    #     part2 (k2:2, k2:2, k3:2, k1:2) shufle => (k2:2, k2:2, k2:2) => k2:6, (k3:2, k3:2) => k3:4

    #     part1 (k1:2, k2:2, k3:2, k1:2) => (k1:4, k2:2, k3:2) =>  shuffle => (k1:2, k1:2, k1:2) => k1:6
    #     part2 (k2:2, k2:2, k3:2, k1:2) => (k1:4, k2:2, k3:2) => shuffle => (k2:2, k2:2, k2:2) => k2:6, (k3:2, k3:2) => k3:4

    #
    # # Or group items to do something else
    print()
    print("== Grouped")
    grouped = codeRows.groupByKey()
    for el in grouped.collect():
        print(str(el))
    #
    print(str(grouped.toDebugString().decode("utf-8")))


    # b'(1) PythonRDD[19] at collect at C:/Users/VOpolskiy/PycharmProjects/another/eas-017-RDD-py/lection/01-RDD.py:208 []\n |
    # MapPartitionsRDD[18] at mapPartitions at PythonRDD.scala:145 []\n |
    # ShuffledRDD[17] at partitionBy at NativeMethodAccessorImpl.java:0 []
    # \n +-(1) PairwiseRDD[16] at groupByKey at C:/Users/VOpolskiy/PycharmProjects/another/eas-017-RDD-py/lection/01-RDD.py:207 []
    # \n    |  PythonRDD[15] at groupByKey at C:/Users/VOpolskiy/PycharmProjects/another/eas-017-RDD-py/lection/01-RDD.py:207 []
    # \n    |  ParallelCollectionRDD[0] at readRDDFromFile at PythonRDD.scala:274 []'
    # # Don't forget about joins with preferred languages
    #
    profileData = [("Ivan", "Java"), ("Elena", "Scala"), ("Petr", "Scala")]
    programmerProfiles = sc.parallelize(profileData)
    #
    print()
    print("== Joined")

    joined = programmerProfiles.join(codeRows)
    print(joined.toDebugString)
    for el in joined.collect():
        print(el)

    # also we can use special operator to group values from both rdd by key
    # also we sort in DESC order
    # co-group is performing grouping in the same executor due to which its performance is always better.

def other_operations():
    profileData = [("Ivan", "Java"), ("Elena", "Scala"), ("Petr", "Scala")]
    programmerProfiles = sc.parallelize(profileData)

    data = [("Ivan", 240), ("Petr", 39), ("Elena", 290), ("Elena", 300)]
    codeRows = sc.parallelize(data)
    codeRows = programmerProfiles.cogroup(codeRows)


    print()
    print("== Cogroup")
    cogroup_result = programmerProfiles.cogroup(codeRows).sortByKey(False).collect()
    for el in cogroup_result:
        print(el)


    # # If required we can get amount of values by each key
    # print()
    # print("== CountByKey")
    # print(str(joined.countByKey()))
    #
    # # or get all values by specific key
    # print()
    # print("== Lookup")
    # print(str(joined.lookup("Elena")))
    #
    # # codeRows keys only
    # print()
    # print("== Keys")

    # for el in codeRows.keys().collect()
    #
    #
    #  # Print values only
    #   println()
    #   println("== Value")
    #   codeRows.values.collect().foreach(println)

    """
    Exercises
        1. Read the movies dataset as an RDD    
        2. Show the distinct genres as an RDD
        3. Print all the movies in the Drama genre with IMDB rating > 6
    """
    df = spark.read.csv("../sources/movies")
    movies_rdd = df.rdd

    dist_movies = movies_rdd.map(lambda row: row.Major_Genre).distinct()
    print(dist_movies)

    # spark_dsl_only_df = col("Major_Genre") == "Drama" && col("IMDB_Rating") > 6
    python_lambda_rdd = lambda movie: movie.Major_Genre & movie.IMDB_Rating > 6

    s_movies = movies_rdd.filter(python_lambda_rdd)
    print(s_movies)



if __name__ == '__main__':
    other_operations()
    sleep(10000)
