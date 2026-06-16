from pyspark import pipelines as dp
from pyspark.sql import functions as sf
from transformations.utilities import paths


def add_full_time(clock_df):
    return (
        clock_df.withColumn("hour_str", sf.lpad(sf.col("hour").cast("string"), 2, "0"))
        .withColumn("minute_str", sf.lpad(sf.col("minute").cast("string"), 2, "0"))
        .withColumn("second_str", sf.lpad(sf.col("second").cast("string"), 2, "0"))
        .withColumn("clock_time", sf.concat_ws(":", "hour_str", "minute_str", "second_str"))
    )


@dp.temporary_view
def hour():
    return (
        spark.range(1)
        .withColumn("hour", sf.explode(sf.sequence(sf.lit(0), sf.lit(23), sf.lit(1))))
        .drop("id")
    )


@dp.temporary_view
def minute():
    return (
        spark.range(1)
        .withColumn("minute", sf.explode(sf.sequence(sf.lit(0), sf.lit(59), sf.lit(1))))
        .drop("id")
    )


@dp.temporary_view
def second():
    return (
        spark.range(1)
        .withColumn("second", sf.explode(sf.sequence(sf.lit(0), sf.lit(59), sf.lit(1))))
        .drop("id")
    )


@dp.table(
    name=paths.CLOCK_PATH,
    comment="Enterprise-grade clock dimension for all 86,400 seconds of the day.",
)
def clock():

    hour_df = spark.read.table("hour")
    minute_df = spark.read.table("minute")
    second_df = spark.read.table("second")

    clock_df = hour_df.crossJoin(minute_df).crossJoin(second_df)

    clock_df = add_full_time(clock_df)

    clock_df = clock_df.withColumn(
        "seconds_since_midnight", sf.col("hour") * 3600 + sf.col("minute") * 60 + sf.col("second")
    )

    clock_df = clock_df.withColumn(
        "time_id", sf.col("hour") * 10000 + sf.col("minute") * 100 + sf.col("second")
    )

    clock_df = clock_df.withColumn("hhmm", sf.col("hour") * 100 + sf.col("minute"))

    clock_df = clock_df.withColumn(
        "time_12h",
        sf.date_format(sf.concat(sf.lit("1970-01-01 "), sf.col("clock_time")), "hh:mm:ss a"),
    )

    clock_df = clock_df.withColumn(
        "am_pm", sf.date_format(sf.concat(sf.lit("1970-01-01 "), sf.col("clock_time")), "a")
    )

    clock_df = clock_df.withColumn(
        "hour_12", sf.date_format(sf.concat(sf.lit("1970-01-01 "), sf.col("clock_time")), "hh")
    )

    clock_df = clock_df.withColumn(
        "time_of_day",
        sf.when((sf.col("hour") >= 5) & (sf.col("hour") < 12), "Morning")
        .when((sf.col("hour") >= 12) & (sf.col("hour") < 17), "Afternoon")
        .when((sf.col("hour") >= 17) & (sf.col("hour") < 21), "Evening")
        .otherwise("Night"),
    )

    clock_df = clock_df.withColumn("is_morning", (sf.col("hour") >= 5) & (sf.col("hour") < 12))
    clock_df = clock_df.withColumn("is_afternoon", (sf.col("hour") >= 12) & (sf.col("hour") < 17))
    clock_df = clock_df.withColumn("is_evening", (sf.col("hour") >= 17) & (sf.col("hour") < 21))
    clock_df = clock_df.withColumn("is_night", (sf.col("hour") >= 21) | (sf.col("hour") < 5))

    clock_df = clock_df.withColumn("minute_of_day", sf.col("hour") * 60 + sf.col("minute"))
    clock_df = clock_df.withColumn("second_of_day", sf.col("seconds_since_midnight"))

    clock_df = clock_df.withColumn("is_sleep_window", (sf.col("hour") >= 22) | (sf.col("hour") < 6))

    clock_df = clock_df.withColumn("is_work_window", (sf.col("hour") >= 9) & (sf.col("hour") < 17))

    clock_df = clock_df.withColumn(
        "is_meal_window",
        sf.when(sf.col("hour").between(6, 9), True)
        .when(sf.col("hour").between(12, 14), True)
        .when(sf.col("hour").between(18, 20), True)
        .otherwise(False),
    )

    return clock_df
