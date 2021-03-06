# -*- coding: utf-8 -*-
"""
Data cleaning methods used in the project.
"""
from datetime import timedelta, datetime
from functools import reduce

from pyspark.sql.dataframe import DataFrame as SparkDataFrame
from pyspark.sql.functions import udf, when

from utils.common import convert_column_type, normalize_column_name


def clean_demographics(df: SparkDataFrame) -> SparkDataFrame:
    """Clean demographics data

    :param df: demographic data frame to be cleaned.
    :return: cleaned demographic data frame
    """

    int_cols = ['count', 'male_population', 'female_population',
                'total_population', 'number_of_veterans', 'foreign_born']
    float_cols = ['median_age', 'average_household_size']
    df = convert_column_type(df, 'integer', int_cols)
    df = convert_column_type(df, 'float', float_cols)
    df = df.fillna(0, [*int_cols, *float_cols])
    return df


def clean_airports(df: SparkDataFrame) -> SparkDataFrame:
    """Clean airport data

    :param df: airport data frame to be cleaned.
    :return: cleaned airport data frame
    """

    df = df.filter(
        (df['iso_country'] == 'US') &
        (df['type'].contains('airport')) &
        (df['local_code'].isNotNull())
    )
    df = df \
        .withColumn('iso_region', df['iso_region'].substr(4, 2)) \
        .withColumn('elevation_ft', df['elevation_ft'].cast('float'))

    return df


def clean_immigration(df: SparkDataFrame) -> SparkDataFrame:
    """Clean immigration data

    :param df: immigration data frame to be cleaned.
    :return: cleaned immigration data frame
    """

    drop_cols = ['visapost', 'occup', 'entdepu', 'insnum', 'count', 'entdepa', 'entdepd', 'matflag', 'dtaddto',
                 'biryear', 'admnum']
    int_cols = ['cicid', 'i94yr', 'i94mon', 'i94cit', 'i94res', 'i94mode', 'i94bir', 'i94visa', 'dtadfile']
    date_cols = ['arrdate', 'depdate']
    date_udf = udf(lambda x: x and (timedelta(days=int(x)) + datetime(1960, 1, 1)).strftime('%Y-%m-%d'))

    df = df.drop(*drop_cols)
    df = convert_column_type(df, 'integer', int_cols)
    for col in date_cols:
        df = df.withColumn(col, date_udf(df[col]))

    # Remove the row if the data in any of fk column is lost
    fk_columns = ['i94cit', 'i94port', 'i94addr']
    df = reduce(lambda df, idx: df.filter(df[fk_columns[idx]].isNotNull()), range(len(fk_columns)), df)

    return df


def clean_temperature(df: SparkDataFrame) -> SparkDataFrame:
    """Clean temperature data

    :param data: temperature data frame to be cleaned.
    :return: cleaned temperature data frame
    """

    df = df.filter(df['AverageTemperature'].isNotNull())
    # rename columns to snake_case
    df = normalize_column_name(df, lambda x: ''.join(['_'+c.lower() if c.isupper() else c for c in x]).lstrip('_'))
    return df


def clean_countries(df: SparkDataFrame) -> SparkDataFrame:
    """Clean countries data

    :param df: countries data frame to be cleaned.
    :return: cleaned countries data frame
    """

    df = convert_column_type(df, 'integer', ['code'])

    # change the name to match the names in demographics for further operations.
    name_to_change = [
        ('MEXICO Air Sea, and Not Reported (I-94, no land arrivals)', 'MEXICO'),
        ('BOSNIA-HERZEGOVINA', 'BOSNIA AND HERZEGOVINA'),
        ('INVALID: CANADA', 'CANADA'),
        ('CHINA, PRC', 'CHINA'),
        ('GUINEA-BISSAU', 'GUINEA BISSAU'),
        ('INVALID: PUERTO RICO', 'PUERTO RICO'),
        ('INVALID: UNITED STATES', 'UNITED STATES')
    ]
    df = reduce(lambda df, idx: df.withColumn(
        'name',
        when(df['name'] == name_to_change[idx][0], name_to_change[idx][1]).otherwise(df['name'])
    ), range(len(name_to_change)), df)

    return df
