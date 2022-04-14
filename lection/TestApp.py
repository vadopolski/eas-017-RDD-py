import sys
from time import sleep

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession. \
    builder. \
    appName("RDD"). \
    getOrCreate()

sc = spark.sparkContext


def rdd_creation():
    # read a file in parallel
    numbers = range(1, 1000000)
    numbers_rdd = sc.parallelize(numbers, 4)
    print(numbers_rdd.collect())


if __name__ == '__main__':
    input = sys.argv[0]

    rdd_creation()
