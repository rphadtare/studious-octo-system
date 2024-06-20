import matplotlib.pyplot as plt
from mysql import connector
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
import pandas as pd


def sample():
    print("This is first python file !!")


def sample_mat_plot_program():
    plt.plot([1, 2, 3], [10, 20, 30])
    plt.show()


def sample_spark_program():
    spark = SparkSession.builder.appName("first").master("local").getOrCreate()
    ids = [i for i in range(0, 100)]
    names = ["Rohit", "Rajani", "Pooja", "Rushi", "Rutu"] * 20

    df = spark.sparkContext.parallelize(ids).toDF(IntegerType()).toDF("id")
    print(type(df), " - count: ", df.count())
    df.show(truncate=False)


def sample_pandas_program():
    ids = [i for i in range(0, 100)]
    names = ["Rohit", "Rajani", "Pooja", "Rushi", "Rutu"] * 20

    id_series = pd.Series(ids, name="id")
    name_series = pd.Series(names, name="values")

    df = pd.concat([pd.DataFrame(id_series), pd.DataFrame(name_series)], axis=1)
    print(df)


def sample_connect_to_db_program():
    mydb = connector.connect(
        host="localhost",
        user="f1gd",
        password="Star@123$",
        database="f1gd"
    )

    print(f"Connection object: {mydb}")

    my_cursor = mydb.cursor()
    my_cursor.execute("select * from F1_GD_NOTIFICATIONS")

    result = my_cursor.fetchall()
    for row in result:
        print(row)

    mydb.close()


def main():
    sample_connect_to_db_program()


if __name__ == "__main__":
    main()
