from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql import functions as F

spark = SparkSession. \
    builder. \
    appName("Data Sources"). \
    master("local"). \
    config("spark.jars", "../jars/postgresql-42.2.19.jar"). \
    config("spark.jars", "../jars/udf_jars.jar"). \
    config("spark.sql.legacy.timeParserPolicy", "LEGACY"). \
    getOrCreate()

# cars_df = spark.read.json("../sources/cars")
def window_functions():
    simpleData = [("James", "Sales", 3000), ("John", "ServiceDesk", 4600), ("Michael", "Sales", 4600), ("Robert", "Sales", 4100),
                     ("Maria", "Finance", 3000), ("James", "Sales", 3000), ("Scott", "Finance", 3300), ("Jen", "Finance", 3900),
                     ("Jeff", "Marketing", 3000), ("Kumar", "Marketing", 2000), ("Saif", "Sales", 4100)]

    employeeDF = spark.createDataFrame(simpleData).toDF("employee_name", "department", "salary")

    employeeDF.createOrReplaceTempView("employee")

    spark.sql("""select distinct salary from (
                    select 
                        employee_name, 
                        department, 
                        salary, 
                        row_number() OVER (ORDER BY salary DESC) as row_num,
                        dense_rank() OVER (ORDER BY salary DESC) as dense_rank,
                        rank() OVER (ORDER BY salary DESC) as rank
                  from employee) where dense_rank = 2""")

    #
    #
    print("Experiment")
    # result_sql_df.show()

    result_sql_df2 = spark.sql("""
            select max(salary) 
                from employee 
            where salary != (select max(salary) from employee)""")
    # result_sql_df2.explain()
    # result_sql_df2.show()

    windowSpec = Window.partitionBy("department").orderBy(F.col("salary").desc())
    result_with_rank_df = employeeDF.\
        withColumn("rank", F.rank().over(windowSpec)).\
        withColumn("dense_rank", F.dense_rank().over(windowSpec))
    # result_with_rank_df.explain()
    # result_with_rank_df.show()


    result_sql_df = spark.sql("""
            select 
                employee_name, 
                department, 
                salary, 
                count(*) OVER () as cnt
            from employee
    """)
    result_sql_df.explain(True)
    result_sql_df.show()

    # windowSpec = Window.orderBy(F.col("salary").desc())
    # single_part_df_1 = employeeDF. \
    #     withColumn("count", count("*"))
    print("DON'T ADD COUNT")
    # single_part_df_1.show()
    # single_part_df_1.explain()

    print("CORRECT WAY")
    cnt = employeeDF.count()
    result_with_count_df = employeeDF.\
        withColumn("count", F.lit(cnt))
    result_with_count_df.show()
    result_with_count_df.explain()

    windowSpec = Window.orderBy(F.col("salary").desc())
    single_part_df_2 = employeeDF.\
        withColumn("row_num", F.row_number().over(windowSpec))
    print("DON'T ADD ROW NUM")
    single_part_df_2.show()
    single_part_df_2.explain()

    result_with_uniq_num = employeeDF.\
        withColumn("row_num", F.monotonically_increasing_id())
    print("CORRECT WAY")
    result_with_uniq_num.show()
    result_with_uniq_num.explain()



# UDF, UDAF


def user_define_functions():
    jvm = spark.sparkContext._jvm.dataframe.employeeFromRepositoryUDF().apply()



    employee_df = spark.read.jdbc("", "")
    depart_df = spark.read.csv("department.csv")

    joined_df = employee_df.join(depart_df, employee_df.dep_id == depart_df.id, "left")

    # depart_df.select(col(depart_df.id))
    # depart_df.select(get_row_by_id_from_employee_id_udf(col(depart_df.id)))
    #
    # get_row_by_id_from_employee_id_udf = udf(some_method_from_repository(""))
    #




    #
    # calendar_df.select(get_days_by_doctor_name_udf("Alex"))


    # Step-1: Define and register UDF function

    lambda_is_world_war_two_year = lambda year: 1939 <= year <= 1945

    # def lambda_is_world_war_two_year(year: Int) {
    #     year <= 1939 && year >= 1945
    # }
    #
    # boolean lambda_is_world_war_two_year(int year) { => 0
    # boolean lambda_is_world_war_two_year(Integer year) {
    #     year <= 1939 && year >= 1945
    # }

    # 1 way
    is_world_war_two_year_udf = udf(lambda_is_world_war_two_year)

    # 2 way
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

    stateNames.\
        select(F.col("Year"), is_world_war_two_year_udf(F.col("Year"))).\
        distinct().\
        show(150)

    stateNames.createOrReplaceTempView("stateNames")
    spark.sql(
        """SELECT DISTINCT Name, Year 
            FROM stateNames 
            WHERE Year IS NOT NULL AND isWorldWarTwoYear(Year) = true 
            ORDER BY Name DESC""").\
        show(150)


if __name__ == '__main__':
    user_define_functions()