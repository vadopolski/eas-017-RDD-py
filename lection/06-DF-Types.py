from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession. \
    builder. \
    appName("Joins"). \
    master("local"). \
    config("spark.jars", "../jars/postgresql-42.2.19.jar"). \
    getOrCreate()

movies_df = spark.read.json("../sources/movies")


def demo_literal_values():
    meaning_of_life_df = movies_df.select(col("Title"), lit(42).alias("MOL"))
    meaning_of_life_df.show()

def demo_booleans():
    drama_filter = movies_df.Major_Genre == "Drama" # column object of TYPE boolean
    good_rating_filter = movies_df.IMDB_Rating > 7.0
    # can use & (and), | (or), ~ (not)
    good_drama_filter = good_rating_filter & drama_filter

    # can use boolean column objects as arguments to filter
    good_dramas_df = movies_df.filter(good_drama_filter).select("Title")

    # can add the col object as a column/property for every row
    movies_with_good_drama_condition_df = movies_df.select(col("Title"), good_drama_filter.alias("IsItAGoodDrama"))
    # can filter using the true/false value of a column
    good_dramas_df_v2 = movies_with_good_drama_condition_df.filter("IsItAGoodDrama")

    # negation
    bad_drama_filter = ~good_drama_filter
    bad_dramas = movies_df.select(col("Title"), bad_drama_filter)
    bad_dramas.show()


def demo_numerical_ops():
    movies_avg_ratings_df = movies_df.select(
        col("Title"),
        (col("Rotten_Tomatoes_Rating") / 10 + col("IMDB_Rating")) / 2
    )
    # can use ==, >=, >, <, <= to obtain boolean col objects


    # Pearson correlation - for numerical fields
    # a number [-1, 1]
    # is an "action" (the DF must be evaluated)
    rating_correlation = movies_df.stat.corr("IMDB_Rating", "Rotten_Tomatoes_Rating")
    print(rating_correlation)

def demo_string_ops():
    movies_df.select(initcap(col("Title"))) # capitalize initials of every word in the string
    # upper(...), lower(...) to uppercase/lowercase
    movies_df.filter(col("Title").contains("love"))


cars_df = spark.read.json("../sources/cars")
def demo_regexes():
    regexString = "volkswagen|vw"
    vw_df = cars_df.select(
        col("Name"),
        regexp_extract(col("Name"), regexString, 0).alias("regex_extract")
    ).filter(col("regex_extract") != "")

    vw_df.show()

    vw_new_name_df = vw_df.select(
        col("Name"),
        regexp_replace(col("Name"), regexString, "Volkswagen").alias("replacement")
    )
    vw_new_name_df.show()

"""
Exercise
    Filter the cars DF, return all cars whose name contains either element of the list
    - contains function
    - regexes
"""

def get_car_names():
    return ["Volkswagen", "Mercedes-Benz", "Ford"]

# v1 - regexes
regexString = "|".join(get_car_names()) # Volkswagen|Mercedes-Benz|Ford
cars_interest_df = cars_df.select(
        col("Name"),
        regexp_extract(lower(col("Name")), regexString, 0).alias("regex_extract")
    ).filter(col("regex_extract") != "").orderBy(col("regex_extract"))



# v2 - contains
from functools import reduce

car_name_filters = [col("Name").contains(car_name.lower()) for car_name in get_car_names()]
big_filter = reduce(lambda filter1, filter2: filter1 | filter2, car_name_filters)
filtered_cars = cars_df.filter(big_filter)


def complex_type():
    movies_with_release_dates_df = movies_df.select(
        col("Title"),
        to_date(col("Release_Date"), "dd-MMM-YY").alias("Actual_Release")
    )

    # date operations
    enriched_movies_df = movies_with_release_dates_df. \
        withColumn("Today", current_date()). \
        withColumn("Right_Now", current_timestamp()). \
        withColumn("Movie_Age", datediff(col("Today"), col("Actual_Release")) / 365)

    # check for empty date
    no_release_known_df = movies_with_release_dates_df.filter(col("Actual_Release").isNull())

    # hypothetical
    movies_with_2_formats = movies_df.select(col("Title"), col("Release_Date")). \
        withColumn("Date_F1", to_date(col("Release_Date"), "dd-MM-yyyy")). \
        withColumn("Date_F2", to_date(col("Release_Date"), "yyyy-MM-dd")). \
        withColumn("Actual_Date", coalesce(col("Date_F1"), col("Date_F2")))

    # structures
    movies_struct_df = movies_df. \
        select(col("Title"), struct(col("US_Gross"), col("Worldwide_Gross"), col("US_DVD_Sales")).alias("Profit")). \
        select(col("Title"), col("Profit").getField("US_Gross").alias("US_Profit"))

    # structures - SQL expression strings
    movies_struct_df_v2 = movies_df. \
        selectExpr("Title", "(US_Gross, Worldwide_Gross, US_DVD_Sales) as Profit"). \
        selectExpr("Title", "Profit.US_Gross as US_Profit")


    # very nested data structures
    movies_struct_df_v3 = movies_df. \
        selectExpr("Title",
                   "((IMDB_Rating, Rotten_Tomatoes_Rating) as Rating, (US_Gross, Worldwide_Gross, US_DVD_Sales) as Profit) as Success"). \
        selectExpr("Title", "Success.Rating.IMDB_Rating as IMDB")
    # movies_struct_df_v3.show()

    # arrays
    movies_with_words_df = movies_df.select(col("Title"), split(col("Title"), " |,").alias("Title_Words"))
    movies_with_words_df.show()
    #                                                     ^^^^^^^^^^^^^^^^^^^^^^^^^ col object of type ARRAY[String]
    # you can have nested arrays

    # array operations
    array_ops_df = movies_with_words_df.select(
        col("Title"),
        expr("Title_Words[0]"),  # the first element in the array
        size(col("Title_Words")),  # the length of the array
        array_contains(col("Title_Words"), "Love")
        # a bunch of array_(...) functions
    )

    array_ops_df = movies_with_words_df.select(
        col("Title"),
        explode(col("Title_Words"))
    )

    array_ops_df.show()


if __name__ == '__main__':
    complex_type()



