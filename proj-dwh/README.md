# Data Warehouse

## Introduction

This project is to help a music streaming startup, Sparkify, to move their processes and data onto cloud.
An ETL pipeline is built to extract their data from S3, stage them in Redshift, and transform them into a set of analytics tables. 

## Datasets

The source datasets are **song dataset** and **log dataset**. Both of them are in json format.


## Data Schema

### Staging tables

* **staging_events**: Actions done by the users.
* **staging_songs**: Songs and artists information.

### Fact tables

* **fact_songplay**: Records in event data associated with song plays.

### Dimension tables

* **dim_users**: Users in the app.
* **dim_songs**: Songs in music database.
* **dim_artist**: Artists in music database.
* **dim_time**: Timestamps of records in songplays broken down into specific units.

## Files

* **create_tables.py**: Drop and create all the tables.
* **etl.py**: ETL pipeline that copies data from s3 to staging tables, and inserts the data into fact and dimension tables.
* **sql_queries.py**: The queries used by **create_tables.py** and **etl.py**.
* **dwh.cfg**: Configuration file.

## Quick Start

1. Create Redshift cluster on aws console.
2. Setup AWS credential information on the local machine.
3. Execute **create_tables.py** to drop and create the tables.
4. Execute **etl.py** to move the data from s3 to Redshift and create the analytic dataset.