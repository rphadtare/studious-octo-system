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
current_dir_path = os.curdir
input_csv_path = f"{current_dir_path}/data/*"
db_prop = dict([("driver", "com.mysql.cj.jdbc.Driver"), ("user", "myapp_user"), ("password", "Star@123$")])


def get_nyc_data():
    df = spark.read.option("header", "True").csv(path=input_csv_path).drop("_c0")
    lower_case_cols = dict([(name, "_".join(str(name).lower().split(" ")).replace("-", "")) for name in df.columns])
    print(lower_case_cols.values())

    df = df.withColumnsRenamed(lower_case_cols)
    # df.printSchema()

    df.write.jdbc(url="jdbc:mysql://localhost/myapp", table="nyc_sales_stg", mode="overwrite", properties=db_prop)

    df.printSchema()
    spark.stop()


def get_distinct_of_every_attributes_nyc_data():
    df = spark.read.jdbc(url="jdbc:mysql://localhost/myapp", table="nyc_sales_stg", properties=db_prop)
    df.show()



def main():
    get_distinct_of_every_attributes_nyc_data()
    spark.stop()


if __name__ == "__main__":
    main()
