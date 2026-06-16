from pyspark import pipelines as dp
from pyspark.sql import functions as sf
from transformations.utilities import constants, utils, paths



dp.temporary_view()
def hour():
    hour = spark.range(1).withColumn(
    "hour",
    sf.explode(
        sf.sequence(
            sf.lit(0), 
            sf.lit(23),
            sf.lit(1)
        )
    )
)




@dp.table(
    name = paths.CLOCK_PATH
    COMMENT = 
    """
    Clock table for all timestamps in the day 
    """
)
def clock():
    hour_df = spark.read.table("hour")
    clock = hour_df
    return clock