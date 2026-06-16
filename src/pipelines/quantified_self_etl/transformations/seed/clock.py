from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as sf
from pyspark.sql.column import Column
from transformations.utilities import paths


def add_full_time(clock_df: DataFrame) -> DataFrame:
    for column in clock_df.columns:
        length_1_condition: Column = sf.length(sf.col(column)) == sf.lit(1)
        clock_df = clock_df.withColumn(column, sf.col(column).cast("string"))
        clock_df = clock_df.withColumn(
            column,
            sf.when(length_1_condition, sf.concat(sf.lit("0"), sf.col(column))).otherwise(
                sf.col(column)
            ),
        )
    time_column: Column = sf.concat_ws(":", sf.col("hour"), sf.col("minute"), sf.col("second"))

    return clock_df.withColumn("clock_time", time_column)


@dp.temporary_view
def hour():
    return (
        spark.range(1).withColumn("hour", sf.explode(sf.sequence(sf.lit(0), sf.lit(23), sf.lit(1))))
    ).drop("id")


@dp.temporary_view
def minute():
    return (
        spark.range(1).withColumn(
            "minute", sf.explode(sf.sequence(sf.lit(0), sf.lit(59), sf.lit(1)))
        )
    ).drop("id")


@dp.temporary_view
def second():
    return (
        spark.range(1).withColumn(
            "second", sf.explode(sf.sequence(sf.lit(0), sf.lit(59), sf.lit(1)))
        )
    ).drop("id")


@dp.table(
    name=paths.CLOCK_PATH,
    comment="""
    Clock table for all timestamps in the day.
    """,
)
def clock():
    hour_df = spark.read.table("hour")
    minute_df = spark.read.table("minute")
    seconds_df = spark.read.table("second")
    clock_df = hour_df.crossJoin(minute_df).crossJoin(seconds_df)

    clock_df = add_full_time(clock_df)
    return clock_df
