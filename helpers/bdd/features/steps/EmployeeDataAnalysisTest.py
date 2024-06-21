from behave import *
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

from helpers.bdd import calculateAvgSalrayPerDept
from helpers.spark_helper import getSparkSession

spark = getSparkSession("Emp Data Test", None)

emp_schema = StructType([
    StructField('employee_id', IntegerType(), True),
    StructField('employee_name', StringType(), True),
    StructField('dept_id', IntegerType(), True),
    StructField('salary', IntegerType(), True),
])

dept_schema = StructType([
    StructField('dept_id', IntegerType(), True),
    StructField('avg_sal_per_dept', FloatType(), True)
])

emptyEmpRDD = spark.sparkContext.emptyRDD()
deptEmptyRDD = spark.sparkContext.emptyRDD()
empDf = spark.createDataFrame(emptyEmpRDD, schema=emp_schema)
expected_df = spark.createDataFrame(deptEmptyRDD, schema=dept_schema)
actual_df = spark.createDataFrame(deptEmptyRDD, schema=dept_schema)

##
# To run BDD example - behave helpers/bdd/features/EmployeeDataAnalysis.feature
# #


@given(u'there is \'employee\' dataframe with following data')
def step_impl(context):
    emp_data = list()
    for row in context.table:
        emp_data.append((int(row["employee_id"]), row["employee_name"], int(row["dept_id"]), int(row["salary"])))

    # print(type(emp_data))
    # print(emp_data)

    global empDf
    empDf = spark.createDataFrame(emp_data, schema=emp_schema)
    empDf.show(truncate=False)


@when(u'calculating average salary per department')
def step_impl(context):
    global actual_df
    actual_df = calculateAvgSalrayPerDept(spark, empDf)
    print("Actual result: ")
    actual_df.show(truncate=False)


@then(u'result is \'avg_sal_per_dept\' dataframe with following lines')
def step_impl(context):
    dept_data = list()
    for row in context.table:
        dept_data.append((int(row["dept_id"]), float(row["avg_sal_per_dept"])))

    global expected_df
    expected_df = spark.createDataFrame(dept_data, schema=dept_schema)

    print("Expected result: ")
    expected_df.show(truncate=False)

    count = expected_df.exceptAll(actual_df).count()
    spark.stop()
    assert count == 0, f"Expected count: 0 is not matched with actual result {count}!!"

