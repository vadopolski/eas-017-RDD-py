from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession. \
    builder. \
    appName("Data Sources"). \
    master("local"). \
    config("spark.jars", "../jars/postgresql-42.2.19.jar"). \
    config("spark.sql.legacy.timeParserPolicy", "LEGACY"). \
    getOrCreate()

cars_df = spark.read.json("../data/cars")

driver = "org.postgresql.Driver"
url = "jdbc:postgresql://localhost:5432/rtjvm"
user = "docker"
password = "docker"


def read_table(table_name):
    return spark.read. \
        format("jdbc"). \
        option("driver", driver). \
        option("url", url). \
        option("user", user). \
        option("password", password). \
        option("dbtable", "public." + table_name). \
        load()


employees_df = read_table("employees")
salaries_df = read_table("salaries")
dept_managers_df = read_table("dept_manager")
dept_emp_df = read_table("dept_emp")
departments_df = read_table("departments")

# save table names
employees_df.createOrReplaceTempView("employees")
salaries_df.createOrReplaceTempView("salaries")
dept_managers_df.createOrReplaceTempView("dept_manager")
dept_emp_df.createOrReplaceTempView("dept_emp")
departments_df.createOrReplaceTempView("departments")


def dml_ddl_expressions():
    # store as a Spark table
    cars_df.createOrReplaceTempView("cars")

    # select American cars
    # regular DF API
    american_cars_df = cars_df.filter(col("Origin") == "USA").select(col("Name"))
    # run SQL queries on top of DFs known to Spark under a certain name
    american_cars_df_v2 = spark.sql("select Name from cars where Origin = 'USA'")  # returns a DF

    # store DFs as Spark tables (files known to Spark)
    # cars_df.write.mode("overwrite").saveAsTable("cars")


"""
1. show all employees and their max salary over time
2. show all employees who were never managers
3. for every employee, find the difference between their salary (current/latest) and the max salary of their department (departments table)
"""


# 181. Employees Earning More Than Their Managers

# 262. Trips and Users

# 184. Department Highest Salary

# 185. Department Top Three Salaries

# 178. Rank Scores

# 180. Consecutive Numbers

# 197. Rising Temperature

# Yandex id  1 2 4 5 6 8
def what_will_happen():
    spark.sql("""
        SELECT t1.id as t1, 
               t2.id as t2 
        FROM t as t1 
        RIGHT JOIN t as t2 ON (t1.id=t2.id+1)
    """)


# transform_table
# Input:
# +------+---------+-------+
# | id   | revenue | month |
# +------+---------+-------+
# | 1    | 8000    | Jan   |
# | 2    | 9000    | Jan   |

# Output:
# +------+-------------+-------------+-------------+-----+-------------+
# | id   | Jan_Revenue | Feb_Revenue | Mar_Revenue | ... | Dec_Revenue |
# +------+-------------+-------------+-------------+-----+-------------+
# | 1    | 8000        | 7000        | 6000        | ... | null        |


# 1384. Total Sales Amount by Year

# 1841. League Statistics

# 177. Nth Highest Salary | 176. Second Highest Salary


if __name__ == '__main__':
    dml_ddl_expressions()
