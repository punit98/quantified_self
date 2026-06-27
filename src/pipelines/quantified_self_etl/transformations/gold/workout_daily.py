from pyspark import pipelines as dp
from pyspark.sql import functions as sf
from transformations.utilities import paths, utils


@dp.table(
    name=paths.WORKOUT_DAILY_PATH,
    comment="""Exercise level detail of my workouts per day.
    Averages the weight and sums the reps.
    Replaces set volume to total volume for taht exercise for that day
    Recalculates weight_per_rep
    """,
)
def workout_exercise_details():

    workout_details = spark.readStream.table(paths.INT_WORKOUT_DETAILS_PATH)

    workout_daily = workout_details.groupBy(
        "calendar_key",
        "utc_offset",
    ).agg(
        sf.min(sf.col("date_time")).alias("start_timestamp"),
        sf.max(sf.col("date_time")).alias("end_timestamp"),
        sf.count(sf.lit(1)).alias("number_of_sets"),
        sf.count(sf.col("muscle_group").distint()).alias("number_of_muscle_groups"),
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

    workout_daily = utils.create_clock_key(workout_daily, "start_timestamp").withColumnRenamed(
        "clock_key", "start_clock_key"
    )
    workout_daily = utils.create_clock_key(workout_daily, "end_timestamp").withColumnRenamed(
        "clock_key", "end_clock_key"
    )

    return workout_daily
