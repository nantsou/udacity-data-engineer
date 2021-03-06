# -*- coding: utf-8 -*-
"""
Data loading methods used in the project.
"""
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame as SparkDataFrame


def load_csv(spark: SparkSession, path: str, delimiter: str = ',') -> SparkDataFrame:
    """Load data into DataFrame from CSV file

    :param spark: spark session object
    :param path: the file path of the csv file
    :param delimiter: the delimiter used in csv file
    :return: the data frame contains loaded data
    """
    return spark.read.format('csv').option('header', 'true').option('sep', delimiter).load(path)


def load_json(spark: SparkSession, path: str) -> SparkDataFrame:
    """Load data into DataFrame from Json file

    :param spark: spark session object
    :param path: the file path of the json file
    :return: the data frame contains loaded data
    """
    return spark.read.json(path)


def load_parquet(spark: SparkSession, path: str) -> SparkDataFrame:
    """Load data into DataFrame from parquet file

    :param spark: spark session object
    :param path: the file path of the parquet file
    :return: the data frame contains loaded data
    """
    return spark.read.parquet(path)
