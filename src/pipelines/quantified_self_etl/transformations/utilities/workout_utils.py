from pyspark.sql import DataFrame
from pyspark.sql import functions as sf


def add_duration_columns(dataframe: DataFrame, start: str, end: str) -> DataFrame:
    """returns the hour, minute and second duration between 2 timestamps

    Args:
        dataframe (DataFrame):
        start_timestamp_col (str):
        end_timestamp_col (str):

    Returns:
        DataFrame: Supplied DataFrame with durations added
    """

    dataframe = dataframe.withColumn(
        "total_hours_duration", sf.timestamp_diff("HOUR", sf.col(start), sf.col(end))
    )

    dataframe = dataframe.withColumn(
        "total_minutes_duration", sf.timestamp_diff("MINUTE", sf.col(start), sf.col(end))
    )

    dataframe = dataframe.withColumn(
        "total_seconds_duration", sf.timestamp_diff("SECOND", sf.col(start), sf.col(end))
    )

    dataframe = dataframe.withColumn(
        "remainder_minutes",
        sf.col("total_minutes_duration") - (sf.col("total_hours_duration") * 60),
    )

    dataframe = dataframe.withColumn(
        "remainder_seconds",
        sf.col("total_seconds_duration") - (sf.col("total_minutes_duration") * 60),
    )

    return dataframe


def add_intensity_metrics(dataframe: DataFrame) -> DataFrame:
    """Adds some intensity metrics to the table

    Args:
        dataframe (DataFrame): dataframe to process

    Returns:
        DataFrame: dataframe with intensity metrics added
    """

    dataframe = dataframe.withColumn(
        "weight_per_rep", sf.col("total_volume") / sf.col("number_of_reps")
    )

    dataframe = dataframe.withColumn(
        "average_reps_per_set", sf.col("number_of_reps") / sf.col("number_of_sets")
    )

    dataframe = dataframe.withColumn(
        "average_volume_per_set", sf.col("total_volume") / sf.col("number_of_sets")
    )

    dataframe = dataframe.withColumn(
        "volume_per_minute", sf.col("total_volume") / sf.col("total_minutes_duration")
    )

    dataframe = dataframe.withColumn(
        "reps_per_minute", sf.col("number_of_reps") / sf.col("total_minutes_duration")
    )

    dataframe = dataframe.withColumn(
        "sets_per_hour", sf.col("number_of_sets") / sf.col("total_hours_duration")
    )

    dataframe = dataframe.withColumn(
        "effort_score", sf.col("total_volume") * sf.col("number_of_sets")
    )

    return dataframe
