from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *

spark = SparkSession. \
    builder. \
    appName("Data Sources"). \
    master("local"). \
    config("spark.jars", "../jars/postgresql-42.2.19.jar"). \
    config("spark.sql.legacy.timeParserPolicy", "LEGACY"). \
    getOrCreate()


def window_functions():
    windowSpec = Window.partitionBy("department").orderBy(col("salary").desc)

    # employeeDF\
    # .withColumn("rank", rank().over(windowSpec))
    # .withColumn("rank", dense_rank().over(windowSpec))
    # .show()

    # !!!!!!!!!!! DANGER CASES
    # !!!!!!!!!!! Single Partition

def user_define_functions():
    # Step-1: Define and register UDF function

    lambda_is_world_war_two_year = lambda year: year >= 1939 & year <= 1945

    spark.sqlContext.udf.register("isWorldWarTwoYear", lambda_is_world_war_two_year)

    # Step-1: Use UDF function
    #!!!!!!!!!!! DANGER CASES

def expressions():
    windowSpec = Window.partitionBy("department").orderBy(col("salary").desc)