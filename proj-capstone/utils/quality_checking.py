# -*- coding: utf-8 -*-
"""
Data modeling methods used in the project.
"""
from typing import Dict, List
from pyspark.sql.dataframe import DataFrame as SparkDataFrame
from pyspark.sql.functions import col, lower, split


def check_existence(df: SparkDataFrame) -> bool:
    """Check given data frame has data.

    :param df: the data frame to be checked
    :return: True if data frame has data otherwise False
    """
    return df.count() > 0


def check_fact_table_integrity(fact: SparkDataFrame, dim_demographics: SparkDataFrame, dim_airports: SparkDataFrame,
                               dim_countries: SparkDataFrame) -> bool:
    """Check the integrity of models based on fact table.

    :param fact: the data frame of fact table, immigration.
    :param dim_demographics: the data frame of dimension demographics table
    :param dim_airports: the data frame of dimension airports table
    :param dim_countries: the data frame of dimension of countries table
    :return: True if all the integrity check pass otherwise False
    """

    integrity_demographics = fact.select(col('i94addr')).distinct()\
                                 .join(dim_demographics,
                                       fact['i94addr'] == dim_demographics['state_code'], 'left_anti').count() == 0

    integrity_airports = fact.select(col('i94port')).distinct()\
                             .join(dim_airports,
                                   fact['i94port'] == dim_airports['local_code'], 'left_anti').count() == 0

    integrity_countries = fact.select(col('i94cit')).distinct().\
                              join(dim_countries, fact['i94cit'] == dim_countries['code'], 'left_anti').count() == 0

    return integrity_demographics & integrity_airports & integrity_countries
