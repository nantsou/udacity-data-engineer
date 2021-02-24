# Data Modeling with Postgres

## Introduction

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

They'd like a data engineer to create a Postgres database with tables designed to optimize queries on song play analysis, and bring you on the project. Your role is to create a database schema and ETL pipeline for this analysis. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

## Project

In this project, you'll apply what you've learned on data modeling with Postgres and build an ETL pipeline using Python. To complete the project, you will need to define fact and dimension tables for a star schema for a particular analytic focus, and write an ETL pipeline that transfers data from files in two local directories into these tables in Postgres using Python and SQL.

## Project Datasets

### Song Dataset

Songs dataset is a subset of [Million Song Dataset](http://millionsongdataset.com/).

**Sample**

```
{
    "num_songs": 1, 
    "artist_id": "ARJIE2Y1187B994AB7", 
    "artist_latitude": null, 
    "artist_longitude": null, 
    "artist_location": "", 
    "artist_name": "Line Renaud", 
    "song_id": "SOUPIRU12A6D4FA1E1", 
    "title": "Der Kleine Dompfaff", 
    "duration": 152.92036, 
    "year": 0
}
```
```
prop_name           prop_len            prop_cnt           prop_mean        prop_max_len        prop_min_len
num_songs                 71                  71                   1                   1                   1
artist_id               1278                  71                  18                  18                  18
artist_latitude          243                  31                   7                   8                   6
artist_longitude         273                  31                   8                  10                   7
artist_location          563                  43                  13                  29                   4
artist_name              953                  71                  13                  94                   3
song_id                 1278                  71                  18                  18                  18
title                   1435                  71                  20                  52                   5
duration                 628                  71                   8                   9                   7
year                     112                  28                   4                   4                   4
```

We can find out that some data of `year`, `artist_latitude` and `artist_longitude` are lost, so that we can have them nullable.
Basically year should be 4 digits so that any value without 4 digits will be treated as lost.

The lengths of `num_songs`, `artist_id` , `song_id` and `year` should be fixed.
The lengths of `artist_latitude` and `artist_longitude` is around 7 ~ 10 with 5 digits in decimal, which latitude is in -90.00000 to 90.00000 and longitude is in -180.00000 to 180.00000.
The length of `duration` is around 7 ~ 9 with 5 digits in decimal, which I assume that all the durations should be less than 1000.00000.
The lengths of `artist_location`, `artist_name` and `title` are more scattered. The max lengths are 29, 94 and 52 respectively.

### Log Dataset

Logs dataset is generated by [Event Simulator](https://github.com/Interana/eventsim).

**Sample**

```
{
    "artist": null,
    "auth": "Logged In",
    "firstName": "Walter",
    "gender": "M",
    "itemInSession": 0,
    "lastName": "Frye",
    "length": null,
    "level": "free",
    "location": "San Francisco-Oakland-Hayward, CA",
    "method": "GET","page": "Home",
    "registration": 1540919166796.0,
    "sessionId": 38,
    "song": null,
    "status": 200,
    "ts": 1541105830796,
    "userAgent": "\"Mozilla\/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"",
    "userId": "39"
}
```
```
prop_name           prop_len            prop_cnt           prop_mean        prop_max_len        prop_min_len
artist                 89063                6820                  13                  89                   2
auth                   72790                8056                   9                  10                   9
firstName              43077                7770                   5                  10                   3
gender                  7770                7770                   1                   1                   1
itemInSession          11691                7115                   1                   3                   1
lastName               46185                7770                   5                   9                   3
length                 60388                6820                   8                  10                   6
level                  32224                8056                   4                   4                   4
location              217824                7770                  28                  46                  10
method                 24168                8056                   3                   3                   3
page                   60590                8056                   7                  16                   4
registration          116550                7770                  15                  15                  15
sessionId              24534                8056                   3                   4                   1
song                  122453                6820                  17                 151                   1
status                 24168                8056                   3                   3                   3
ts                    104728                8056                  13                  13                  13
userAgent             836396                7770                 107                 139                  63
userId                 15501                7770                   1                   3                   1
```

The total amount of data is 8056 so that we can find out that some properties lose some data, like `artist`, `firstName`, `lastName`, `gerder` and so on, which we can make them nullable.
The lengths of `gender`, `level`, `method`, `status` and `ts` may be fixed. But we know that the values for usual methods are `GET`, `POST`, `PUT` and `DELETE`.
Besides, we found that there are many data loss.

## Quick Start

```bash
# create the database and the tables.
python create_tables.py

# execute the etl
python etl.py
```

## Scripts

**sql_queries.py**: All the queries used in create_tables.py and etl.py. 

**create_tables.py**: Clean previous database and create the database and schema.

**etl.py**: Read the song data and log data in JSON format and insert these data into database.

## Schema

### Fact Tables

**songplays**: Records in log data associated with song plays. And we focus on the record of the page with "NextSong"

```
songplay_id (primary key), start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
```

| songplay_id | start_time                 | user_id | level | song_id | artist_id | session_id | location                            | user_agent                                                                                                              |
|-------------|----------------------------|---------|-------|---------|-----------|------------|-------------------------------------|-------------------------------------------------------------------------------------------------------------------------|
| 1           | 2018-11-29 00:00:57.796000 | 73      | paid  | -       | -         | 954        | Tampa-St. Petersburg-Clearwater, FL | "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit/537.78.2 (KHTML, like Gecko) Version/7.0.6 Safari/537.78.2" |

We set the `ts` in log data to start_time.

---

### Dimension Tables

**users**: users in the app.

```
user_id (primary key), first_name, last_name, gender, level
```

| user_id | first_name | last_name | gender | level |
|---------|------------|-----------|--------|-------|
| 79      | James      | Martin    | M      | free  |

---

**songs**: song information from song dataset.

```
song_id (primary key), title, artist_id, year, duration
```

| song_id            | title                          | artist_id          | year | duration  |
|--------------------|--------------------------------|--------------------|------|-----------|
| SOFFKZS12AB017F194 | A Higher Place (Album Version) | ARBEBBY1187B9B43DB | 1994 | 236.17261 |

---

**artists**: artist information from song dataset.

```
artist_id (primary key), name, location, latitude, longitude
```

| artist_id          | name      | location        | lattitude | longitude |
|--------------------|-----------|-----------------|-----------|-----------|
| ARNNKDK1187B98BBD5 | Jinx      | Zagreb Croatia  | 45.80726  | 15.9676   |

---

**time**: timestamps of records in songplays broken down into specific units

```
start_time, hour, day, week, month, year, weekday
```

| start_time                 | hour | day | week | month | year | weekday |
|----------------------------|------|-----|------|-------|------|---------|
| 2018-11-29 00:00:57.796000 | 0    | 29  | 48   | 11    | 2018 | 3       |