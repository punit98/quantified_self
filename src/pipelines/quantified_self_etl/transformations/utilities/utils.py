from pyspark.sql import DataFrame
from pyspark.sql import functions as sf
from pyspark.sql.types import StructType


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


def create_calendar_key(dataframe: DataFrame, timestamp_column: str):
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
