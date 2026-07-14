from pyspark import pipelines as dp
from pyspark.sql import types
from pyspark.sql.types import StructField, StructType
from transformations.utilities import paths, utils

stg__workoutlog_schema = StructType(
    [
        StructField("date_time", types.TimestampType(), False),
        StructField("utc_offset", types.StringType(), False),
        StructField("muscle_group", types.StringType(), False),
        StructField("exercise", types.StringType(), False),
        StructField("variation", types.StringType(), False),
        StructField("weight", types.FloatType(), False),
        StructField("drop_weight", types.FloatType(), True),
        StructField("second_drop_weight", types.FloatType(), True),
        StructField("reps", types.IntegerType(), False),
        StructField("drop_reps", types.IntegerType(), True),
        StructField("second_drop_reps", types.IntegerType(), True),
        StructField("source", types.StringType(), True),
        StructField("calendar_key", types.StringType(), False),
        StructField("clock_key", types.StringType(), False),
        StructField("_rescued_data", types.StringType(), False),
    ]
)
ddl_schema = utils.struct_to_ddl(stg__workoutlog_schema)


@dp.table(
    name=paths.STG__WORKOUTLOG_PATH,
    comment="""
        The staging table for workouts with cleaned columns, correct datatypes and deduplication
        """,
    schema=ddl_schema,
)
def stg__workoutlog():
    raw_workoutlog = spark.readStream.table(paths.RAW_WORKOUTLOG_PATH)

    lower_case_columns = ["muscle_group", "exercise", "variation"]
    raw_workoutlog = utils.convert_column_values_to_lower_case(raw_workoutlog, lower_case_columns)

    raw_workoutlog = utils.preserve_timezone(raw_workoutlog, "date_time")
    raw_workoutlog = utils.type_cast_columns(raw_workoutlog, ["date_time"], "timestamp")

    float_columns = ["weight", "drop_weight", "second_drop_weight"]
    raw_workoutlog = utils.type_cast_columns(
        raw_workoutlog, column_list=float_columns, column_type="float"
    )

    int_columns = ["reps", "drop_reps", "second_drop_reps"]
    raw_workoutlog = utils.type_cast_columns(
        raw_workoutlog, column_list=int_columns, column_type="int"
    )

    stg__workoutlog = raw_workoutlog.dropDuplicates().fillna(
        0,
        subset=[
            "weight",
            "drop_weight",
            "second_drop_weight",
            "reps",
            "drop_reps",
            "second_drop_reps",
        ],
    )

    stg__workoutlog = utils.prepare_for_export(
        dataframe=stg__workoutlog, timestamp_column="date_time", source_name="workoutlog"
    )

    return stg__workoutlog
