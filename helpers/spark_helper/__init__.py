import pyspark
from pyspark.sql import SparkSession, DataFrame, functions


def getSparkSession(appName, conf):
    if conf is not None:
        return SparkSession.builder.config(conf=conf). \
            appName(appName).master("local[*]").getOrCreate()
    else:
        return SparkSession.builder.appName(appName).master("local[*]").getOrCreate()


##
# To get values of specific column of dataframe as a string
# #
def get_all_distinct_column_values_of_column_in_str(dataframe: DataFrame, column_name, sep):
    rdd = dataframe.select(column_name).distinct().rdd.map(lambda record: str(record[0]))
    return f"{sep}".join(rdd.collect())

##
# To get dataframe from specific data format
# #
