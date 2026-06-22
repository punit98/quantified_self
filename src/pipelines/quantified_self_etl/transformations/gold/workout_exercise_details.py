from pyspark import pipelines as dp
from pyspark.sql import types, functions as sf
from pyspark.sql.types import StructField, StructType
from transformations.utilities import paths, utils

workout_set_details_schema = StructType(
    [
        StructField("date_time", types.TimestampType(), False),
        StructField("utc_offset", types.StringType(), False),
        StructField("calendar_key", types.StringType(), False),
        StructField("clock_key", types.StringType(), False),
        StructField("muscle_group", types.StringType(), False),
        StructField("exercise", types.StringType(), False),
        StructField("variation", types.StringType(), False),
        StructField("weight", types.FloatType(), False),
        StructField("drop_weight", types.FloatType(), True),
        StructField("second_drop_weight", types.FloatType(), True),
        StructField("reps", types.IntegerType(), False),
        StructField("drop_reps", types.IntegerType(), True),
        StructField("second_drop_reps", types.IntegerType(), True),
        StructField("set_volume", types.FloatType(), True),
        StructField("average_weight", types.DoubleType(), True),
        StructField("total_reps", types.IntegerType(), True),
        StructField("weight_per_rep", types.DoubleType(), True),
        StructField("source", types.StringType(), True),
        StructField("_rescued_data", types.StringType(), False),
    ]
)
ddl_schema = utils.struct_to_ddl(workout_set_details_schema)


@dp.table(
    name=paths.WORKOUT_EXERCISE_DETAILS,
    comment="""Exercise level detail of my workouts per day.
    Averages the weight and sums the reps.
    Replaces set volume to total volume for taht exercise for that day
    Recalculates weight_per_rep
    """,
    schema=ddl_schema,
)
def workout_exercise_details():

    workout_details = spark.readStream.table(paths.INT_WORKOUT_DETAILS)

    pass
