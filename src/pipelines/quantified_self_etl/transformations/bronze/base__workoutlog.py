from pyspark import pipelines as dp
from transformations.utilities import paths
from transformations.utilities import utils
from pyspark.sql import types as types
from pyspark.sql.types import StructField, StructType
from pyspark.sql import functions as sf
import json


stg__workoutlog_schema = StructType([
    StructField("date_time", types.TimestampType(), False),
    StructField("muscle_group", types.StringType(), False),
    StructField("exercise", types.StringType(), False),
    StructField("variation", types.StringType(), False),
    StructField("weight", types.FloatType(), False),
    StructField("drop_weight", types.FloatType(), True),
    StructField("second_drop_weight", types.FloatType(), True),
    StructField("reps", types.IntegerType(), False),
    StructField("drop_reps", types.IntegerType(), True),
    StructField("second_drop_reps", types.IntegerType(), True),
])
ddl_schema = utils.struct_to_ddl(stg__workoutlog_schema)


@dp.table(
        name=paths.STG__WORKOUTLOG_PATH,
        comment="""
        The base table for workouts with cleaned columns, correct datatypes and deduplication
        """,
        schema = ddl_schema
        )
def base__workoutlog():
    raw_workoutlog = spark.readStream.table(paths.RAW_WORKOUTLOG_PATH)

    raw_workoutlog = raw_workoutlog.withColumn(
        "date_time",
        sf.to_timestamp(sf.col("date_time"), "dd MMM yyyy 'at' HH:mm")
        )
        

    lower_case_columns = ["muscle_group", "exercise", "variation"]
    raw_workoutlog = utils.convert_column_values_to_lower_case(raw_workoutlog, lower_case_columns)

    float_columns = ["weight", "drop_weight", "second_drop_weight"]
    raw_workoutlog = utils.type_cast_columns(raw_workoutlog, column_list = float_columns, column_type = "float")

    int_columns = ["reps", "drop_reps", "second_drop_reps"]
    raw_workoutlog = utils.type_cast_columns(raw_workoutlog, column_list = int_columns, column_type = "int")

    raw_workoutlog = raw_workoutlog.select([field.name for field in stg__workoutlog_schema.fields])

    stg__workoutlog = raw_workoutlog.dropDuplicates().fillna(0, subset = ["weight", "drop_weight", "second_drop_weight", "reps", "drop_reps", "second_drop_reps"])


    return stg__workoutlog



