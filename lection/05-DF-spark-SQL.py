from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession. \
    builder. \
    appName("Data Sources"). \
    master("local"). \
    config("spark.jars", "../jars/postgresql-42.2.19.jar"). \
    config("spark.sql.legacy.timeParserPolicy", "LEGACY"). \
    getOrCreate()

def dml_ddl_expressions():
    cars_df = spark.read.json("../sources/cars")

    american_cars_df = cars_df.filter(col("Origin") == "Japan").select(col("Name"))
    print("REGULAR DF API")
    american_cars_df.show()

    # store as a Spark table dont write data to storage EXTERNAL TABLE
    #  DataFrame => SQL metastore
    cars_df.createOrReplaceTempView("cars")
    
    # run SQL queries on top of DFs known to Spark under a certain name
    print("SQL metastore => DataFrame")
    american_cars_df_v2 = spark.sql("SELECT Name FROM cars WHERE Origin = 'Japan'")
    american_cars_df_v2.show()

    # DROPPING EXTERNAL TABLE DOES NOT DELETE DATA, ONLY IN METASTORE
    print("DROPPING EXTERNAL TABLE")
    spark.sql("DROP TABLE cars")
    # spark.sql("SELECT Name FROM cars WHERE Origin = 'Japan'")

    # store DFs as Spark tables (files known to Spark) to Spark storage HDFS
    #  DataFrame => SQL metastore + Spark storage
    print("SAVING TO MANAGED TABLE")
    cars_df.write.mode("overwrite").saveAsTable("cars_managed_table")

    print("READ FROM MANAGED STORAGE 1")
    american_cars_df_v2 = spark.sql("SELECT * FROM cars_managed_table")
    american_cars_df_v2.show()

    print("READ FROM MANAGED STORAGE 2")
    cars_managed_df = spark.table("cars_managed_table")
    cars_managed_df.show()

    print("DROPPING MANGED TABLE")
    spark.sql("DROP TABLE cars_managed_table")

    print("DDL DML INSIDE TABLE")
    spark.sql("""CREATE SCHEMA test""")

    spark.sql("CREATE TABLE test.students (name VARCHAR(64), address VARCHAR(64)) USING PARQUET PARTITIONED BY (student_id INT)")

    spark.sql("INSERT INTO test.students VALUES('Bob Brown', '456 Taylor St, Cupertino', 222222),('Cathy Johnson', '789 Race Ave, Palo Alto', 333333)")

    spark.sql("SELECT * FROM test.students").show()

    print("DROPPING MANGED TABLE")
    spark.sql("DROP TABLE test.students")

     # https://spark.apache.org/docs/latest/sql-ref-syntax-dml-insert-into.html



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

"""
1. show all employees and their max salary over time
2. show all employees who were never managers
3. for every employee, find the difference between their salary (current/latest) and the max salary of their department (departments table)
"""

def exersices():
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
