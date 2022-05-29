from time import sleep

from pyspark.sql import SparkSession, Window
from pyspark.sql.types import *
from pyspark.sql.functions import *


spark = SparkSession. \
    builder. \
    config("spark.sql.autoBroadcastJoinThreshold", 0). \
    appName("Joins"). \
    master("local"). \
    getOrCreate()

def join_optimisation():
    crime_facts = spark \
        .read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("../sources/crimes/crime.csv")

    crime_facts.cache().count()
    crime_facts.show()

    grouped_crime_df = crime_facts.\
        groupBy(col("OFFENSE_CODE")).\
        count().\
        filter(col("OFFENSE_CODE") == 1402)

    print("Moving Filter")
    grouped_crime_df.explain(True)
    grouped_crime_df.show()

    offense_сodes = spark.\
        read.\
        option("header", "true").\
        option("inferSchema", "true").\
        csv("../sources/crimes/offense_codes.csv")

    offense_сodes.show(100, False)

    print("Sort Merge Join")
    rob_sort_merge_df = crime_facts.\
        join(offense_сodes, col("CODE") == col("OFFENSE_CODE")).\
        filter(col("NAME").startswith("ROBBERY")).\
        groupBy(col("NAME")).\
        count().\
        orderBy(col("count").desc())

    rob_sort_merge_df.explain(True)
    rob_sort_merge_df.show()

    print("Broad Cast Join")
    rob_broadcast_df = crime_facts.\
        join(broadcast(offense_сodes), col("CODE") == col("OFFENSE_CODE")).\
        filter(col("NAME").startswith("ROBBERY")).\
        groupBy(col("NAME")).\
        count().\
        orderBy(col("count").desc())

    rob_broadcast_df.explain(True)
    rob_broadcast_df.show()

    codes_total = offense_сodes.\
        filter(col("NAME").startsWith("ROBBERY")).\
        select(col("CODE")).\
        distinct().\
        count()

    bf = offense_сodes.filter(col("NAME").startsWith("ROBBERY"))



if __name__ == '__main__':
    join_optimisation()
    sleep(10000)
