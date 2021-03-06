# -*- coding: utf-8 -*-
"""
Common methods used in the project.
"""
import re
from functools import reduce
from typing import List, Tuple

from pandas.core.frame import DataFrame as PandasDataFrame
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame as SparkDataFrame


camel_case_pattern = re.compile(r'(?<!^)(?=[A-Z])')


def create_spark_session() -> SparkSession:
    """Create and return a spark session object.

    :return: a spark session object
    """

    spark = SparkSession \
        .builder \
        .config('spark.jars.packages', 'saurfang:spark-sas7bdat:2.0.0-s_2.11,org.apache.hadoop:hadoop-aws:2.7.0') \
        .enableHiveSupport() \
        .getOrCreate()
    return spark


def print_data(df: SparkDataFrame, row_count: int = 10) -> PandasDataFrame:
    """Print the data count and data content of given spark data frame.

    :param df: the spark data frame to show the data
    :param row_count: the number of data to print
    :return: the truncated and transformed data frame with row_count
    """

    print(f'data count: {df.count()}')
    return df.limit(row_count).toPandas()


def convert_column_type(df: SparkDataFrame, data_type: str, cols: List[str]) -> SparkDataFrame:
    """Convert given columns to given data type.

    :param df: the spark data frame to be converted
    :param data_type: the data type to be used.
    :param cols: the column list to be converted
    :return: Converted spark data frame
    """

    for col in [col for col in cols if col in df.columns]:
        df = df.withColumn(col, df[col].cast(data_type))
    return df


def normalize_column_name(df: SparkDataFrame, normalize_func: callable) -> SparkDataFrame:
    """Normalize the column name to snake_case with the given function.

    :param df: the spark data frame with the column name to be normalized
    :param normalize_func: the function used to normalize column name
    :return: the spark data frame with normalized column name
    """
    columns = df.columns
    df = reduce(lambda df, idx: df.withColumnRenamed(
        columns[idx],
        normalize_func(columns[idx])
    ), range(len(columns)), df)

    return df


def rename_column_name(df: SparkDataFrame, name_set_list: List[Tuple[str, str]]) -> SparkDataFrame:
    """Rename the columns with given column names.

    :param df: the spark data frame with the columns to be renamed
    :param name_set_list: the list of tuple contains old name and new name
    :return: the renamed spark data frame
    """
    df = reduce(lambda df, idx: df.withColumnRenamed(
        name_set_list[idx][0],
        name_set_list[idx][1]
    ), range(len(name_set_list)), df)
    return df
