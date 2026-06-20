from pyspark.sql import DataFrame
from pyspark.sql import functions as sf
from pyspark.sql.types import StructType
from pyspark.sql.column import Column
from typing import Callable


def convert_column_values_to_lower_case(dataframe: DataFrame, columns_list: list[str]) -> DataFrame:
    """
    Converts the values in the supplied columns to lower case.

    Args:
        dataframe (DataFrame): The dataframe.
        columns_list (list[str]): List of  columns whose values need to be converted to lower case.

    Returns:
        DataFrame: DataFrame with values converted to lower case
    """
    for column in columns_list:
        dataframe = dataframe.withColumn(column, sf.lower(sf.col(column)))

    return dataframe


def struct_to_ddl(schema: StructType) -> str:
    """
    Converts the schema Struct to a ddl schema to be passed into a decorator.

    Args:
        schema (StructType): The dataframe.

    Returns:
        ddl_schema: Struct converted into a ddl string
    """
    return ",\n".join(
        [f"{field.name} {field.dataType.simpleString().upper()}" for field in schema.fields]
    )


def type_cast_columns(dataframe: DataFrame, column_list: list[str], column_type: str) -> DataFrame:
    """
    Casts the columns to the supplied type.

    Args:
        dataframe (DataFrame): The dataframe.
        column_list (list[str]): List of columns to be casted.
        column_type (str): The type to which the columns need to be casted.

    Returns:
        DataFrame: DataFrame with columns casted to the supplied type.
    """
    for column in column_list:
        dataframe = dataframe.withColumn(column, sf.col(column).cast(column_type))

    return dataframe


def create_calendar_key(dataframe: DataFrame, timestamp_column: str) -> DataFrame:
    """Generates the calendar+key for supplied dataframe form the supplied timestamp column
        The calendar_key will be in yyyymmdd format
    Args:
        datefarme (DataFrame): dataframe
        timestamp_column (str): timestamp column
    """
    dataframe = dataframe.withColumn(
        "calendar_key", sf.date_format(sf.col(timestamp_column), "yyyyMMdd")
    )

    return dataframe


def create_clock_key(dataframe: DataFrame, timestamp_column: str) -> DataFrame:
    """Generates the calendar+key for supplied dataframe form the supplied timestamp column
        The calendar_key will be in HHmmss format
    Args:
        datefarme (DataFrame): dataframe
        timestamp_column (str): timestamp column
    """
    dataframe = dataframe.withColumn(
        "clock_key", sf.date_format(sf.col(timestamp_column), "HHmmss")
    )

    return dataframe


def preserve_timezone(dataframe: DataFrame, timestamp_col: str) -> DataFrame:
    """Takes a timestamo column which is in the format `yyyy-MM-ddTHH:mm:ssXXX`

    Args:
        dataframe (DataFrame): the dataframe
        timestamp_col (str): the name of the timestamp column

    Returns:
        DataFrame: Original dataframe with timestamo converted to UTC time and the timezone column preserved
    """
    dataframe = dataframe.withColumn("utc_offset", sf.substring(timestamp_col, -6, 6))

    return dataframe


def prepare_for_export(
    dataframe: DataFrame,
    timestamp_column: str,
    source_name: str = "",
    select_columns: list[str] = "*",
):
    """returns the dataframe ready to export to UC

    Args:
        dataframe (DataFrame): dataframe to export
        timestamp_column (str): the timestamo column to create `calendar_key` and `clock_key`
        source_name (str, optional): String that you wnat to be added as the source for this table. Defaults to "".
        select_columns (list[str], optional): the list of columns that you want to select in the output. Defaults to "*".

    Returns:
        _type_: _description_
    """
    dataframe = dataframe.withColumn("source", sf.lit(source_name))
    dataframe = create_calendar_key(dataframe, timestamp_column)
    dataframe = create_clock_key(dataframe, timestamp_column)
    dataframe = dataframe.select(select_columns)
    return dataframe


def average_without_zeros(dataframe: DataFrame, columns_list: list[Column], output_col_name: str):
    """Returns dataframe with the list of columns averaged without including zeros in the average.

    Args:
        dataframe (DataFrame): the Dataframe to transform
        columns_list (list[Column]): list of columns to average
        output_col_name (str): name of the output average column

    Returns:
        DataFrame: Dataframe with average column added
    """
    average = sum(sf.when(sf.col(column) != 0, sf.col(column)) for column in columns_list) / sum(
        sf.when(sf.col(column) != 0, 1).otherwise(0) for column in columns_list
    )
    dataframe = dataframe.withColumn(output_col_name, average)
    return dataframe


def apply_transformation_steps(dataframe: DataFrame, *functions: Callable) -> DataFrame:
    """Returns dataframe with transformation steps applied in order that they are supplied in.
        Supply list of transformation functions using `functools.partial`.
    Args:
        dataframe (DataFrame): Dataframe to transforms
        *functions (Callable): list of transformation functions to apply to dataframe

    Returns:
        DataFrame: Transformaed dataframe
    """
