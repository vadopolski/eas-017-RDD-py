from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql import functions as F

spark = SparkSession. \
    builder. \
    appName("Data Sources"). \
    master("local"). \
    config("spark.jars", "../jars/postgresql-42.2.19.jar"). \
    config("spark.sql.legacy.timeParserPolicy", "LEGACY"). \
    getOrCreate()

# cars_df = spark.read.json("../sources/cars")
def window_functions():
    simpleData = [("James", "Sales", 3000), ("John", "ServiceDesk", 4600), ("Michael", "Sales", 4600), ("Robert", "Sales", 4100),
                     ("Maria", "Finance", 3000), ("James", "Sales", 3000), ("Scott", "Finance", 3300), ("Jen", "Finance", 3900),
                     ("Jeff", "Marketing", 3000), ("Kumar", "Marketing", 2000), ("Saif", "Sales", 4100)]

    employeeDF = spark.createDataFrame(simpleData).toDF("employee_name", "department", "salary")

    employeeDF.show()

    employeeDF.createOrReplaceTempView("employee")

    result_sql_df = spark.sql("select employee_name, department, salary, dense_rank() OVER (ORDER BY salary DESC) as rank from employee")
    result_sql_df.explain()
    result_sql_df.show()

    # windowSpec = Window.partitionBy("department").orderBy(F.col("salary").desc())
    windowSpec = Window.orderBy(F.col("salary").desc())
    result_with_rank_df = employeeDF.\
        withColumn("rank", F.rank().over(windowSpec)).\
        withColumn("dense_rank", F.dense_rank().over(windowSpec))
    result_with_rank_df.explain()
    result_with_rank_df.show()


    single_part_df_1 = employeeDF.\
        withColumn("count", F.count().over(windowSpec))
    print("DON'T ADD COUNT")
    single_part_df_1.show()

    # cnt = employeeDF.count()
    result_with_count_df = employeeDF.\
        withColumn("count", F.lit(employeeDF.count()))
    print("CORRECT WAY")
    result_with_count_df.show()
    result_with_count_df.explain()


    single_part_df_2 = employeeDF.\
        withColumn("row_num", F.row_number().over(windowSpec))
    print("DON'T ADD ROW NUM")
    single_part_df_2.show()
    result_with_count_df.explain()

    result_with_uniq_num = employeeDF.\
        withColumn("row_num", F.monotonically_increasing_id())
    print("CORRECT WAY")
    result_with_uniq_num.show()
    result_with_uniq_num.explain()



def user_define_functions():
    # Step-1: Define and register UDF function

    lambda_is_world_war_two_year = lambda year: year >= 1939 & year <= 1945

    is_world_war_two_year = udf(lambda_is_world_war_two_year)

    spark.udf.register("isWorldWarTwoYear", lambda_is_world_war_two_year)

    stateNames = spark.read.\
        option("header", "true").\
        option("inferSchema", "true").\
        csv("../sources/statenames")

    stateNames.show()

    stateNames.\
        selectExpr("Year", "isWorldWarTwoYear(Year)").\
        distinct().\
        show(150)

    stateNames.select(F.col("Year"), is_world_war_two_year(F.col("Year"))).distinct().show(150)

    stateNames.createOrReplaceTempView("stateNames")

    spark.sql(
        "SELECT DISTINCT Name FROM stateNames WHERE Gender = 'M' and isWorldWarTwoYear(Year) ORDER BY Name DESC").show(150)


if __name__ == '__main__':
    user_define_functions()