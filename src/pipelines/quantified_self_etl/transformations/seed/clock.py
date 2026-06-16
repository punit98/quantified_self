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
    )


@dp.table(
    name=paths.CLOCK_PATH,
    comment="""
    Clock table for all timestamps in the day 
    """
)
def clock():
    hour_df = spark.read.table("hour")
    return hour_df
