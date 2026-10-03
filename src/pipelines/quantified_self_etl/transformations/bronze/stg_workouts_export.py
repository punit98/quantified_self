import pyspark.sql.functions as sf

from transformations.utilities import paths, utils

stg__workout_export_schema = StructType(
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
ddl_schema = utils.struct_to_ddl(paths.RAW_WORKOUTS_EXPORT_PATH)


@dp.table(name=paths.STG__WORKOUTS_EXPORT_PATH)
def workouts():

    return (
        spark.readStream.table()
        .select(sf.explode("data.workouts").alias("workout"))
        .select(
            "workout.id",
            "workout.name",
            "workout.start",
            "workout.end",
            "workout.duration",
            "workout.location",
            "workout.isIndoor",
            "workout.distance.qty",
            "workout.distance.units",
            "workout.activeEnergyBurned.qty",
            "workout.avgHeartRate.qty",
            "workout.maxHeartRate.qty",
        )
    )
