import os

from pyspark.sql import SparkSession, DataFrame, functions
from pyspark.sql.types import StringType

from helpers.spark_helper import getSparkSession, get_all_distinct_column_values_of_column_in_str

spark = getSparkSession("Data analysis")


def demo_of_get_str_values():
    city = ["Lausanne", "Zurich", "St. Gallen", "Bern", "Geneva"] * 5
    df = spark.createDataFrame(city, StringType()).toDF("city")
    print(get_all_distinct_column_values_of_column_in_str(df, "city", ":"))


def demo_of_handling_data_in_csv_format():
    """Handling CSV file data using spark"""

    ##
    # Reading text file to save same
    # #
    current_dir_path = os.curdir
    # print(f"current dir path: {path}")
    df = spark.read.option("header", "True").csv(path=f"{current_dir_path}/resources/employee/employee.txt")
    df.show(51, truncate=False)
    print(f"Total count of dataframe - {df.count()}")
    df.printSchema()


def main():
    demo_of_handling_data_in_csv_format()


if __name__ == "__main__":
    main()
