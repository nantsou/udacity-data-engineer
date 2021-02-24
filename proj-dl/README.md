# Data Lake

## Introduction

This project is to help a music streaming startup, Sparkify, to move their processes and data onto cloud.
An ETL pipeline is built with Spark to load the data from S3, and transform them into a set of analytics tables,
then output the processed data in parquet file and upload them to another S3 bucket.

## Datasets

The source datasets are **song dataset** and **log dataset**. Both of them are in json format.

## Data Schema

### Fact tables files

* **songplay.parquet**: Records in event data associated with song plays.

### Dimension tables files

* **users.parquet**: Users in the app.
* **songs.parquet**: Songs in music database.
* **artists.parquet**: Artists in music database.
* **time.parquet**: Timestamps of records in songplays broken down into specific units.

## Files

* **etl.py**: ETL pipeline that loads the data from s3, then process the data and then upload the data to another s3 bucket.
* **dl.cfg**: Configuration file.

## Quick Start

1. Install the required python package, `pyspark`.
2. Set the information of data input/output and aws access id/key in dl.cfg
3. Execute **etl.py** to run the etl pipeline.
