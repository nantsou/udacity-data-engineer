import configparser

# CONFIG

config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "drop table if exists staging_events"
staging_songs_table_drop = "drop table if exists staging_songs"
songplay_table_drop = "drop table if exists fact_songplays"
user_table_drop = "drop table if exists dim_users"
song_table_drop = "drop table if exists dim_songs"
artist_table_drop = "drop table if exists dim_artists"
time_table_drop = "drop table if exists dim_time"

# CREATE TABLES

staging_events_table_create = ("""
create table if not exists staging_events (
    event_id int identity(0, 1) not null primary key,
    artist varchar distkey,
    auth varchar,
    firstName varchar,
    gender varchar,
    itemInSession integer,
    lastName varchar,
    length float,
    level varchar,
    location varchar,
    method varchar(4),
    page varchar,
    registration varchar,
    sessionId integer,
    song varchar sortkey,
    status integer,
    ts bigint not null,
    userAgent varchar,
    userId integer
) diststyle key;
""")

staging_songs_table_create = ("""
create table if not exists staging_songs (
    num_songs integer not null sortkey,
    artist_id varchar distkey,
    artist_latitude float,
    artist_longitude float,
    artist_location varchar,
    artist_name varchar not null,
    song_id varchar not null,
    title varchar not null,
    duration float not null,
    year integer not null
) diststyle key;
""")

songplay_table_create = ("""
create table if not exists fact_songplays (
    songplay_id integer identity(0, 1) not null primary key,
    start_time timestamp not null sortkey,
    user_id integer not null distkey,
    level varchar not null,
    song_id varchar not null,
    artist_id varchar not null,
    session_id integer not null,
    location varchar not null,
    user_agent varchar not null
) diststyle key;
""")

user_table_create = ("""
create table if not exists dim_users (
    user_id integer not null primary key sortkey,
    first_name varchar not null,
    last_name varchar not null,
    gender varchar(1) not null,
    level varchar not null
) diststyle all;
""")

song_table_create = ("""
create table if not exists dim_songs (
    song_id varchar not null primary key sortkey,
    title varchar not null,
    artist_id varchar not null distkey,
    year integer not null,
    duration float not null
) diststyle key;
""")

artist_table_create = ("""
create table if not exists dim_artists (
    artist_id varchar not null primary key,
    name varchar not null,
    location varchar,
    latitude float,
    longitude float
) diststyle all;
""")

time_table_create = ("""
create table if not exists dim_time (
    start_time timestamp not null primary key sortkey,
    hour integer not null,
    day integer not null,
    week integer not null,
    month integer not null,
    year integer not null,
    weekday integer not null
) diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events 
    from {}
    iam_role {}
    json {}
    region 'us-west-2';
""").format(config.get('S3', 'LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""
    copy staging_songs
    from {}
    iam_role {}
    json 'auto'
    region 'us-west-2';
""").format(config.get('S3', 'SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""
insert into fact_songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
select 
    TIMESTAMP 'epoch' + (se.ts/1000 * interval '1 second'),
    se.userId,
    se.level,
    ss.song_id,
    ss.artist_id,
    se.sessionId,
    se.location,
    se.userAgent
from staging_events se
join staging_songs ss
on se.song = ss.title and se.artist = ss.artist_name and se.length = ss.duration;
""")

user_table_insert = ("""
insert into dim_users(user_id, first_name, last_name, gender, level)
select 
    distinct userId,
    firstName,
    lastName,
    gender,
    level
from staging_events
where userId is not null;
""")

song_table_insert = ("""
insert into dim_songs(song_id, title, artist_id, year, duration)
select
    distinct song_id,
    title,
    artist_id,
    year,
    duration
from staging_songs
where song_id is not null;
""")

artist_table_insert = ("""
insert into dim_artists(artist_id, name, location, latitude, longitude)
select 
    distinct artist_id,
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude
from staging_songs
where artist_id is not null;
""")

time_table_insert = ("""
insert into dim_time(start_time, hour, day, week, month, year, weekday)
with temp_ts_table as (select TIMESTAMP 'epoch' + (ts/1000 * interval '1 second') as ts FROM staging_events)
select
    distinct ts,
    extract(hour from ts),
    extract(day from ts),
    extract(week from ts),
    extract(month from ts),
    extract(year from ts),
    extract(weekday from ts)
from temp_ts_table
where ts is not null;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create,
                        user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop,
                      song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert,
                        time_table_insert]
