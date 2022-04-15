
from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.\
    builder. \
    appName("Introduction to Spark"). \
    master("local"). \
    getOrCreate()

# 1. HOW TO CREATE DATAFRAME
def demo_first_df():
    # specify a schema automatically
    first_df = spark.read. \
        format("json"). \
        option("inferSchema", "true"). \
        load("../sources/cars")

    first_df.show()
    first_df.printSchema()


def demo_manual_schema():
    # specify a schema manually
    cars_schema = StructType([
        StructField("Name", StringType()),
        StructField("Acceleration", DoubleType()),
        StructField("Cylinders", LongType()),
        StructField("Displacement", DoubleType()),
        StructField("Horsepower", LongType()),
        StructField("Miles_per_Gallon", DoubleType()),
        StructField("Origin", StringType()),
        StructField("Weight_in_lbs", LongType()),
        StructField("Year", StringType()),
    ])

    # reading a DF with a manual schema
    cars_manual_schema_df = spark.read. \
        format("json"). \
        schema(cars_schema). \
        load("../sources/cars")

    cars_manual_schema_df.show()

# 2. CASHING


# 3. Catalyst

#  DAG => Parsed Plan => Analyzed plan => Opt plan => Physical plan => Codegeneration




if __name__ == '__main__':
    demo_manual_schema()
