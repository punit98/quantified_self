from pyspark import pipelines as dp
from transformations.utilities import paths
from transformations.utilities import utils
from pyspark.sql import types as types
from pyspark.sql.types import StructField, StructType
from pyspark.sql import functions as sf
import json


base__workoutlog_schema = StructType([
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
schema_json = json.dumps(base__workoutlog_schema.jsonValue())

@dp.table(
        name=paths.BASE__WORKOUTLOG_PATH,
        comment="""
        The base table for workouts with cleaned columns, correct datatypes and deduplication
        """,
        schema = schema_json
        )
def base__workoutlog():
    raw_workoutlog = spark.readStream.table(paths.RAW_WORKOUTLOG_PATH)

    raw_workoutlog = (
        raw_workoutlog
        .withColumn("date_time", sf.to_timestamp(sf.regexp_replace("date_time", "at ", " ")))
    )

    lower_case_columns = ["muscle_group", "exercise", "variation"]
    raw_workoutlog = utils.convert_column_values_to_lower_case(raw_workoutlog, lower_case_columns)

    base__workoutlog = raw_workoutlog.dropDuplicates()


    return base__workoutlog



