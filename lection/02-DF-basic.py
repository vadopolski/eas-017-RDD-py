from time import sleep

from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col
from pyspark.sql.types import *

spark = SparkSession. \
    builder. \
    appName("Introduction to Spark"). \
    master("local"). \
    getOrCreate()


spark.conf.set("spark.sql.shuffle.partitions", 14)


# config("spark.sql.codegen.comments", "true"). \


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

    #  What is data frame - its a set of operation, which transorm data

    # specify a schema manually
    cars_schema = StructType([
        StructField("Name", StringType()),
        StructField("Acceleration", DoubleType()),
        StructField("Cylinders", IntegerType()),
        StructField("Displacement", DoubleType()),
        StructField("Horsepower", IntegerType()),
        StructField("Miles_per_Gallon", DoubleType()),
        StructField("Origin", StringType()),
        StructField("Weight_in_lbs", LongType()),
        StructField("Year", StringType()),
    ])

    #!!!!!!!!!!! Very Important Warning Long Type vs IntType

    # reading a DF with a manual schema
    cars_df = spark.read. \
        format("json"). \
        schema(cars_schema). \
        load("../sources/cars")

    cars_df.show()

    # cars_df.filter(col("test") > 7)
    #
    # cars_df.show()
    # Data Frame comparing

    # most_powered_df.explain(True)

    # Parsed Plan = dont check fields, functions, expression
    # Analyzed plan =
    # Opt logical plan = 30-40 optimisations,
    # Physical plan =

    #     comparing
    #  1 ==> first_df == first_df_inferSchema
    #  2 ==> count1 union + distinct  first_df first_df_inferSchema, count1 = 10, count2 = 10,  count1 union (union all) + distinct = union = 20 or 10, antijoin
    #  3 ==>

    first_df_inferSchema = spark.read. \
        format("json"). \
        option("inferSchema", "true"). \
        load("../sources/cars")

    # first_df_inferSchema.printSchema()
    # cars_df.printSchema()
    # assert(first_df_inferSchema.schema == cars_df.schema)

    # 3. Catalyst

    #  DAG => Parsed Plan => Analyzed plan => Opt plan => Physical plan => Codegeneration
    # Logical Plans
    # Parsed Plan
    # Analyzed plan
    # Opt plan

    print(cars_df.rdd.getNumPartitions())

    most_powered_df = cars_df. \
        where(cars_df.Cylinders > 4). \
        withColumn("new", expr("Acceleration + 10")). \
        sort(cars_df.Horsepower.desc(), cars_df.Acceleration.asc())

    most_powered_df.explain(True)

    print(most_powered_df.rdd.getNumPartitions())
    most_powered_df.show()



    # most_powered_df.queryExecution().debug().codegen()



    # spark.sessionState.sqlParser.parsePlan("SELECT test_udf(id) FROM space WHERE bar = 0")

    # FilterExec -   физический оператор вызывается когда применяем фильтр например
    # ProjectExec - проджектшн
    # Exchange -
    #  childSparkPlan   - два метода execute - под капотом запускает doExecute, каждый физ оператор его реализует
    # там есть child.execute(оператор нижний этажом)
    #     основой любого РДД является

    #    метод doExecute - это цепочка преобразований над RDD[ExternalRow]
    #   метод doConsume - отвечает за кодогенерацию
    #     spark.sql.codegen.wholeStage
    #     spark.sql.codegen


# work with Row()
#








if __name__ == '__main__':
    demo_manual_schema()
    sleep(100000)
