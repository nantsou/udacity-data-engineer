# -*- coding: utf-8 -*-
"""
Data modeling methods used in the project.
"""
from pyspark.sql.dataframe import DataFrame as SparkDataFrame
from pyspark.sql.functions import isnull, udf, when


def model_dim_demographics(df: SparkDataFrame, path: str) -> None:
    """Model dimension demographics data.
    Write data into parquet file.

    :param df: the demographics data frame
    :param path: the path of parquet file to write
    """
    df.write.mode('overwrite').parquet(path)


def model_dim_airports(df: SparkDataFrame, path: str) -> None:
    """Model dimension airports data.
    Write data into parquet file.

    :param df: the demographics data frame
    :param path: the path of parquet file to write
    """
    df.write.mode('overwrite').parquet(path)


def model_dim_countries(df_countries: SparkDataFrame, df_temperature: SparkDataFrame, path: str) -> None:
    """Model dimension countries data
    Join countries data and temperature data
    Write data into parquet file

    :param df_countries: countries data frame
    :param df_temperature: temperature data frame
    :param path: the path of parquet file to write
    """
    # join the table
    df = df_countries.join(df_temperature, 'lower_name', how='left')
    # normalize the name
    title_case_udf = udf(lambda x: x and x.title())
    df = df.withColumn('name', when(isnull(df['name']), title_case_udf(df['lower_name'])).otherwise(df['name']))
    # drop duplicated columns
    df = df.drop('lower_name', 'country')

    # write the data to parquet file
    df.write.mode('overwrite').parquet(path)


def model_fact_immigration(df: SparkDataFrame, dim_demographic: SparkDataFrame, dim_airports: SparkDataFrame,
                           countries: SparkDataFrame, path: str) -> None:
    """Model fact immigration data.
    Remove the data row from immigration data frame by joining dimension tables with 'left_semi'
    Write data into parquet file.

    :param df: the demographics data frame
    :param dim_demographic: the dimension demographic data frame.
    :param dim_airports: the dimension airports data frame.
    :param countries: the transformed countries table.
    :param path: the path of parquet file to write
    """
    fact = df\
        .join(dim_demographic, df['i94addr'] == dim_demographic['state_code'], 'left_semi')\
        .join(dim_airports, df['i94port'] == dim_airports['local_code'], 'left_semi')\
        .join(countries, df['i94cit'] == countries['code'], 'left_semi')

    fact.write.partitionBy('arr_year', 'arr_month', 'arr_day').mode('overwrite').parquet(path)
