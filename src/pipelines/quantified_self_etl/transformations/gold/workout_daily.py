from pyspark import pipelines as dp
from pyspark.sql import functions as sf, DataFrame
from transformations.utilities import paths, utils


def add_duration_columns(dataframe: DataFrame):
    dataframe = dataframe.withColumn("hour_duration",
        sf.timestamp_diff("HOUR", sf.col("start_timestamp"), sf.col("end_timestamp"))
    )

    dataframe = dataframe.withColumn("total_minute_duration",
        sf.timestamp_diff("MINUTE", sf.col("start_timestamp"), sf.col("end_timestamp"))
    )

    dataframe = dataframe.withColumn("remainder_minutes",
        (sf.col("total_minute_duration") - (sf.col("hour_duration") * 60))
    )
    return dataframe


def add_intensity_metrics(dataframe: DataFrame):
    dataframe = dataframe.withColumn(
        "weight_per_rep",
        sf.col("weight") / sf.col("total_reps"),
    )
    return dataframe



@dp.table(
    name=paths.WORKOUT_DAILY_PATH,
    comment="""Day level detail of my workouts per day.
    Averages the weight and sums the reps.
    Replaces set volume to total volume for taht exercise for that day
    Recalculates weight_per_rep
    """,
)
def workout_exercise_details():

    workout_details = spark.read.table(paths.INT_WORKOUT_DETAILS_PATH)

    workout_daily = workout_details.groupBy(
        "calendar_key",
        "utc_offset",
    ).agg(
        sf.min(sf.col("date_time")).alias("start_timestamp"),
        sf.max(sf.col("date_time")).alias("end_timestamp"),
        sf.count(sf.lit(1)).alias("number_of_sets"),
        sf.countDistinct(sf.col("muscle_group")).alias("number_of_muscle_groups"),
        sf.mode(sf.col("muscle_group")).alias("primary_muscle_group"),
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
