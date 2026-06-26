from pyspark import pipelines as dp
from pyspark.sql import functions as sf
from pyspark.sql import types
from pyspark.sql.types import StructField, StructType
from transformations.utilities import paths, utils

workout_set_details_schema = StructType(
    [
        StructField("calendar_key", types.StringType(), False),
        StructField("utc_offset", types.StringType(), False),
        StructField("muscle_group", types.StringType(), False),
        StructField("exercise", types.StringType(), False),
        StructField("variation", types.StringType(), False),
        StructField("start_timestamp", types.TimestampType(), False),
        StructField("end_timestamp", types.TimestampType(), False),
        StructField("number_of_sets", types.LongType(), False),
        StructField("average_weight", types.DoubleType(), False),
        StructField("number_of_reps", types.LongType(), True),
        StructField("max_weight", types.FloatType(), True),
        StructField("min_weight", types.FloatType(), True),
        StructField("max_reps", types.IntegerType(), True),
        StructField("min_reps", types.IntegerType(), True),
        StructField("total_volume", types.DoubleType(), True),
        StructField("max_volume", types.FloatType(), True),
        StructField("min_volume", types.FloatType(), True),
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

    exercise_details = workout_details.groupBy(
        "calendar_key",
        "utc_offset",
        "muscle_group",
        "exercise",
        "variation",
    ).agg(
        sf.min(sf.col("date_time")).alias("start_timestamp"),
        sf.max(sf.col("date_time")).alias("end_timestamp"),
        sf.count(sf.lit(1)).alias("number_of_sets"),
        sf.mean(sf.col("average_weight")).alias("average_weight"),
        sf.sum(sf.col("total_reps")).alias("number_of_reps"),
        sf.max(sf.col("weight")).alias("max_weight"),
        sf.min(sf.col("weight")).alias("min_weight"),
        sf.max(sf.col("total_reps")).alias("max_reps"),
        sf.min(sf.col("total_reps")).alias("min_reps"),
        sf.sum(sf.col("set_volume")).alias("total_volume"),
        sf.max(sf.col("set_volume")).alias("max_volume"),
        sf.min(sf.col("set_volume")).alias("min_volume"),
    )

    return exercise_details
