from pyspark import pipelines as dp
from pyspark.sql import functions as sf
from transformations.utilities import constants, utils, paths


@dp.temporary_view
def hour():
    return (
        spark.range(1)
        .withColumn(
            "hour",
            sf.explode(
                sf.sequence(
                    sf.lit(0),
                    sf.lit(23),
                    sf.lit(1)
                )
            )
        )
    ).drop("id")

@dp.temporary_view
def minutes():
    return (
        spark.range(1)
        .withColumn(
            "minutes",
            sf.explode(
                sf.sequence(
                    sf.lit(0),
                    sf.lit(59),
                    sf.lit(1)
                )
            )
        )
    ).drop("id")


@dp.temporary_view
def seconds():
    return (
        spark.range(1)
        .withColumn(
            "seconds",
            sf.explode(
                sf.sequence(
                    sf.lit(0),
                    sf.lit(59),
                    sf.lit(1)
                )
            )
        )
    ).drop("id")


@dp.table(
    name=paths.CLOCK_PATH,
    comment="""
    Clock table for all timestamps in the day 
    """
)
def clock():
    hour_df = spark.read.table("hour")
    minute_df = spark.read.table("minutes")
    seconds_df = spark.read.table("seconds")
    clock_df = hour_df.crossJoin(minute_df).crossJoin(seconds_df)
    return clock_df
