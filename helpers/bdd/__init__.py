from pyspark.sql import *

from helpers.spark_helper import getSparkSession


def calculateAvgSalrayPerDept(spark: SparkSession, employeeDf: DataFrame):
    return employeeDf.groupby("dept_id").avg("salary").alias("avg_sal_per_dept")
