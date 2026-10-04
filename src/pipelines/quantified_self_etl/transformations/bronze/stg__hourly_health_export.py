from pyspark import pipelines as dp
from pyspark.sql import types, DataFrame, Column
from pyspark.sql.types import StructField, StructType
from transformations.utilities import paths, utils


def extract_type_and_unit(dataframe: DataFrame, metric_column: Column):
    metric_type = None
    unit = None

    # Extract text between [...]
    start = metric_column.find("[")
    end = metric_column.find("]")
    if start != -1 and end != -1 and end > start:
        metric_type = metric_column[start + 1:end]

    # Extract text between (...)
    start = metric_column.find("(")
    end = metric_column.find(")")
    if start != -1 and end != -1 and end > start:
        unit = metric_column[start + 1:end]

    dataframe = dataframe.withcolumn("metric_type", metric_type)
    dataframe = dataframe.withcolumn("unit", unit)
    return dataframe 



@dp.table(
    name=paths.STG__HOURLY_HEALTH_EXPORT_PATH,
    comment = """
    The staging table for hourly health exports
    """
)
def stg__hourly_health_export():
    raw_hourly_health_export = spark.readStream.table(paths.RAW_HOURLY_HEALTH_EXPORT_PATH)

    raw_hourly_health_export = utils.preserve_timezone(raw_hourly_health_export, "Date/Time")

    non_date_columns = [column for column in raw_hourly_health_export.columns if column != "Date/Time"]

    for column in non_date_columns:
        raw_hourly_health_export = raw_hourly_health_export.withColumn(column, raw_hourly_health_export[column].cast(types.DoubleType())
        )
    hourly_health_export = raw_hourly_health_export.unpivot(
        'Date/Time',
        non_date_columns,
        "metric",
        "value"
    )
    hourly_health_export = hourly_health_export.withColumnRenamed("Date/Time", "timestamp")

    hourly_health_export = extract_type_and_unit(hourly_health_export, sf.col("metric"))
    
    hourly_health_export = utils.prepare_for_export(
        dataframe = hourly_health_export,
        timestamp_column = "timestamp",
        source_name = "hourly_health_export"
    )

    return hourly_health_export