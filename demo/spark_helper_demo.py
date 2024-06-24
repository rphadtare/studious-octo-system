import os

import pyspark
from pyspark.sql import SparkSession, DataFrame, functions
from pyspark.sql.functions import to_date, col, concat, lit, length, max
from pyspark.sql.types import StringType, IntegerType, DoubleType

from helpers.spark_helper import getSparkSession, get_all_distinct_column_values_of_column_in_str

conf = pyspark.SparkConf()
conf.set(
    "spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.5.1,mysql:mysql-connector-java:8.0.33",
)

# in case of avro demo
spark = getSparkSession("Data analysis", conf)

# in case of non avro
# spark = getSparkSession("Data analysis", None)

current_dir_path = os.curdir
input_csv_path = f"{current_dir_path}/resources/employee/employee.*"


def demo_of_get_str_values():
    city = ["Lausanne", "Zurich", "St. Gallen", "Bern", "Geneva"] * 5
    df = spark.createDataFrame(city, StringType()).toDF("city")
    print(get_all_distinct_column_values_of_column_in_str(df, "city", ":"))


def demo_of_handling_data_in_csv_format():
    """Handling CSV file data using spark"""

    ##
    # Reading text file to save same
    # #
    # print(f"current dir path: {path}")
    df = spark.read.option("header", "True").csv(path=input_csv_path)
    df.show(51, truncate=False)
    print(f"Total count of dataframe - {df.count()}")
    df.printSchema()


def demo_of_parquet_write():
    """Storing of data in parquet format"""
    df = spark.read.option("header", "True").csv(path=input_csv_path)
    df = df.withColumn("HIRE_DATE", to_date("HIRE_DATE", "dd-MMM-yy"))

    df.show(51, False)
    (df.write.format("parquet").mode(saveMode="overwrite")
     .save(f"{current_dir_path}/resources/employee/parquet/"))


def demo_of_avro_write():
    """Storing of data in avro format"""
    df = spark.read.parquet(f"{current_dir_path}/resources/employee/parquet/")
    df = df.withColumn("email_id", concat("EMAIL", lit("@gmail.com")))

    df.show(51, False)
    df.write.format("avro").mode(saveMode="overwrite") \
        .save(f"{current_dir_path}/resources/employee/avro/")


def demo_read_avro_data():
    """Reading of data from avro format and storing it into json format"""
    df = spark.read.format("avro").load(f"{current_dir_path}/resources/employee/avro/")
    df.show(51, truncate=False)
    df.printSchema()

    df.coalesce(1).write.format("json").mode(saveMode="overwrite") \
        .save(f"{current_dir_path}/resources/employee/json/")


def demo_read_json_data():
    """Reading of data from avro format and storing it into json format"""
    df = spark.read.format("json").load(f"{current_dir_path}/resources/employee/json/")
    cols = [("length_of_" + column_name, length(column_name)) for column_name in df.columns]
    d = dict(cols)
    df = df.withColumns(d)
    # length_cols = [i for i in df.columns if "length_of_" in i]

    max_length_cols = [f"max({column_name}) as max_{column_name}" for column_name in d.keys()]
    df.selectExpr(max_length_cols).show(truncate=False)

    drop_cols = [str(column_name) for column_name in df.columns if "length_of_" in str(column_name)]
    print(drop_cols)
    for i in drop_cols:
        df = df.drop(i)

    df = df.withColumn("EMPLOYEE_ID", col("EMPLOYEE_ID").cast(IntegerType())) \
        .withColumn("SALARY", col("SALARY").cast(DoubleType())) \
        .withColumn("MANAGER_ID", col("MANAGER_ID").cast(IntegerType())) \
        .withColumn("DEPARTMENT_ID", col("DEPARTMENT_ID").cast(IntegerType()))

    df.printSchema()

    d = dict([("driver", "com.mysql.cj.jdbc.Driver"), ("user", "myapp_user"), ("password", "Star@123$")])
    df.write.jdbc(url="jdbc:mysql://localhost/myapp", table="employee", mode="overwrite", properties=d)


def main():
    demo_read_json_data()
    spark.stop()


if __name__ == "__main__":
    main()
