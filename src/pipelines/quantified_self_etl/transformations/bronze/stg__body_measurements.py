from pyspark import pipelines as dp
from pyspark.sql import types
from pyspark.sql.types import StructField, StructType
from transformations.utilities import paths, utils


stg__body_measurements_schema = StructType(
    [
        StructField("date_time", types.TimestampType(), False),
        StructField("utc_offset", types.StringType(), False),
        StructField("is_pump", types.StringType(), False),
        StructField("chest", types.FloatType(), False),
        StructField("shoulder", types.FloatType(), False),
        StructField("left_bicep", types.FloatType(), False),
        StructField("left_forearm", types.FloatType(), False),
        StructField("right_bicep", types.FloatType(), False),
        StructField("right_forearm", types.FloatType(), False),
        StructField("waist", types.FloatType(), False),
        StructField("belly", types.FloatType(), False),
        StructField("glutes", types.FloatType(), False),
        StructField("left_thigh", types.FloatType(), False),
        StructField("left_calf", types.FloatType(), False),
        StructField("right_thigh", types.FloatType(), False),
        StructField("right_calf", types.FloatType(), False),
        StructField("weight", types.FloatType(), False),
        StructField("height", types.FloatType(), False),
        StructField("neck", types.FloatType(), False),
        StructField("source", types.StringType(), True),
        StructField("calendar_key", types.StringType(), False),
        StructField("clock_key", types.StringType(), False),
        StructField("_rescued_data", types.StringType(), False),
    ]
)
ddl_schema = utils.struct_to_ddl(stg__body_measurements_schema)

@dp.table(
    name = paths.STG__BODY_MEASUREMENTS_PATH,
    comment = 
    """
    The staging table for body measurements with cleaned columns, correct datatypes and deduplication
    """,
    schema = ddl_schema
)
def stg_body_measurements():
    raw_body_measurements = spark.readStream.table(paths.RAW_BODY_MEASUREMENTS_PATH)

    raw_body_measurements = utils.convert_column_values_to_lower_case(raw_body_measurements, ["is_pump"])

    raw_body_measurements = utils.preserve_timezone(raw_body_measurements, "date_time")

    raw_body_measurements = raw_body_measurements.withColumnRenamed("tond", "belly").withColumnRenamed("ass", "glutes")

    raw_workoutlog = utils.type_cast_columns(raw_workoutlog, ["date_time"], "timestamp")

    float_columns: list[str] = ["chest", "shoulder", "left_bicep", "left_forearm", "right_bicep", "right_forearm", "waist", "belly", "glutes", "left_thigh", "left_calf", "right_thigh", "right_calf", "weight", "height", "neck"]
    raw_body_measurements = utils.type_cast_columns(raw_body_measurements, column_list=float_columns, column_type="float")

    stg__body_measurements = raw_body_measurements.dropDuplicates().fillna(
        0, subset = float_columns
    )

    stg__body_measurements = utils.prepare_for_export(dataframe = stg__body_measurements, timestamp_column="date_time", source_name="body_measurements")

    return stg__body_measurements