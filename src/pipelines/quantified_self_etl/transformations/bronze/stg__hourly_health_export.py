from pyspark import pipelines as dp
from pyspark.sql import types
from pyspark.sql.types import StructField, StructType
from transformations.utilities import paths, utils

@dp.table(
    name=paths.STG__HOURLY_HEALTH_EXPORT_PATH
    COMMENT = """
    The staging table for hourly health exports
    """
)
def stg__hourly_health_export():
    raw_hourly_health_export = spark.readStream.table(paths.RAW_HOURLY_HEALTH_EXPORT_PATH)

    raw_hourly_health_export = utils.preserve_timezone(raw_hourly_health_export, "Date/Time")

    hourly_health_export = raw_hourly_health_export.unpivot(
        'Date/Time',
        [column for column in raw_hourly_health_export.columns if column != "Date/Time"],
        "metric",
        "value"
    )

    return hourly_health_export