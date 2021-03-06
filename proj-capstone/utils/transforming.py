# -*- coding: utf-8 -*-
"""
Data transforming methods used in the project.
"""
from pyspark.sql.dataframe import DataFrame as SparkDataFrame
from pyspark.sql.functions import col, lower, split

from utils.common import rename_column_name


def transform_demographics(df: SparkDataFrame) -> SparkDataFrame:
    """Transform demographic data
    Pivot the race to present each in the separate columns.
    Aggregate the data to get the data at state level.

    :param df: demographics data frame to be transformed.
    :return: transformed demographics data frame.
    """

    # The first aggregation is to get the information of race pivot.
    aggregated = df.groupBy(['city', 'state', 'state_code']).agg({
        'median_age': 'first',
        'male_population': 'first',
        'female_population': 'first',
        'total_population': 'first',
        'number_of_veterans': 'first',
        'foreign_born': 'first',
        'average_household_size': 'first'
    })

    pivot = df.groupBy(['city', 'state', 'state_code']).pivot('race').sum('count')

    aggregated = aggregated.join(other=pivot, on=['city', 'state', 'state_code'], how='inner')
    aggregated = rename_column_name(
        aggregated,
        [
            ('first(median_age)', 'median_age'),
            ('first(male_population)', 'male_population'),
            ('first(female_population)', 'female_population'),
            ('first(total_population)', 'total_population'),
            ('first(number_of_veterans)', 'number_of_veterans'),
            ('first(foreign_born)', 'foreign_born'),
            ('first(average_household_size)', 'average_household_size'),
            ('Hispanic or Latino', 'hispanic_or_latino'),
            ('Black or African-American', 'black_or_african_american'),
            ('American Indian and Alaska Native', 'american_indian_and_alaska_native'),
            ('White', 'white'),
            ('Asian', 'asian')
        ]
    )

    # The second aggregation is to obtain the data at state level.
    df = aggregated.groupBy(['state', 'state_code']).agg({
        'male_population': 'sum',
        'female_population': 'sum',
        'total_population': 'sum',
        'number_of_veterans': 'sum',
        'foreign_born': 'sum',
        'median_age': 'avg',
        'average_household_size': 'avg',
        'hispanic_or_latino': 'sum',
        'black_or_african_american': 'sum',
        'american_indian_and_alaska_native': 'sum',
        'white': 'sum',
        'asian': 'sum'
    })

    df = rename_column_name(
        df,
        [
            ('sum(male_population)', 'male_population'),
            ('sum(female_population)', 'female_population'),
            ('sum(total_population)', 'total_population'),
            ('sum(number_of_veterans)', 'number_of_veterans'),
            ('sum(foreign_born)', 'foreign_born'),
            ('avg(median_age)', 'median_age'),
            ('avg(average_household_size)', 'average_household_size'),
            ('sum(hispanic_or_latino)', 'hispanic_or_latino'),
            ('sum(black_or_african_american)', 'black_or_african_american'),
            ('sum(american_indian_and_alaska_native)', 'american_indian_and_alaska_native'),
            ('sum(white)', 'white'),
            ('sum(asian)', 'asian')
        ]
    )

    return df


def transform_countries(df: SparkDataFrame) -> SparkDataFrame:
    """Transform countries data
    Add a column of lower case country name for joining purpose

    :param df: the countries data frame to be transformed
    :return: the transformed countries frame
    """

    df = df.withColumn('lower_name', lower(df['name']))
    return df


def transform_temperature(df: SparkDataFrame) -> SparkDataFrame:
    """Transform temperature data
    Aggregate the data to obtain the temperature at country level
    Add a column of lower case country name for joining purpose

    :param df: the temperature data frame to be transformed
    :return: the transformed data frame
    """

    df = df.groupBy(['country']).agg({
        'average_temperature': 'avg',
        'latitude': 'first',
        'longitude': 'first'
    })
    df = rename_column_name(df, [
        ('avg(average_temperature)', 'average_temperature'),
        ('first(latitude)', 'latitude'),
        ('first(longitude)', 'longitude'),
    ])
    df = df.withColumn('lower_name', lower(df['country']))

    return df


def transform_immigration(df: SparkDataFrame) -> SparkDataFrame:
    """Transform immigration data
    Extract year, month and day from arrived date for partitioning purpose

    :param df: the immigration data frame to be transformed
    :return: the transformed immigration data frame
    """

    df = df.withColumn('split_arrdate', split(col('arrdate'), '-'))\
        .withColumn('arr_year', col('split_arrdate')[0])\
        .withColumn('arr_month', col('split_arrdate')[1])\
        .withColumn('arr_day', col('split_arrdate')[2])\
        .drop('split_arrdate')

    return df
