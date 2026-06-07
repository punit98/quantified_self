from pyspark.sql import functions as sf
from pyspark.sql import DataFrame





def convert_column_values_to_lower_case(dataframe: DataFrame, columns_list: list[str]) -> DataFrame:
    """
    Convers the values in the supplied columns to lower case.
    
    Args:
        dataframe (DataFrame): The dataframe.
        columns_list (list[str]): List of  columns whose values need to be converted to lower case.

    Returns:
        DataFrame: DataFrame with values converted to lower case
    """    
    for column in columns_list:
        dataframe = dataframe.withColumn(column, sf.lower(sf.col(column)))

    return dataframe