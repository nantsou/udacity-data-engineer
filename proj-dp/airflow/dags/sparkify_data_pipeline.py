from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                               LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

default_args = {
    'owner': 'nantsou.liu',
    'start_date': datetime.now(),
    'depends_on_past': False,
    'catchup': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('sparkify_data_pipeline',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly'
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_events',
    s3_path='s3://udacity-dend/log_data',
    s3_region='us-west-2',
    json_path='s3://udacity-dend/log_json_path.json'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_songs',
    s3_path='s3://udacity-dend/song_data',
    s3_region='us-west-2',
    json_path='auto'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='songplays',
    sql=SqlQueries.songplay_table_insert,
    append_only=False
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='users',
    sql=SqlQueries.user_table_insert,
    append_only=False
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='songs',
    sql=SqlQueries.song_table_insert,
    append_only=False
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='artists',
    sql=SqlQueries.artist_table_insert,
    append_only=False
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='time',
    sql=SqlQueries.time_table_insert,
    append_only=False
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    dq_checks=[
        {'sql': 'select count(*) from songplays', 'value': 0, 'operator': 'gt'},
        {'sql': 'select count(*) from songs', 'value': 0, 'operator': 'gt'},
        {'sql': 'select count(*) from artists', 'value': 0, 'operator': 'gt'},
        {'sql': 'select count(*) from time', 'value': 0, 'operator': 'gt'},
        {'sql': 'select count(*) from users', 'value': 0, 'operator': 'gt'},
        {'sql': 'select count(*) from songplays where playId is null', 'value': 0, 'operator': 'eq'},
        {'sql': 'select count(*) from songs where songid is null', 'value': 0, 'operator': 'eq'},
        {'sql': 'select count(*) from artists where artistid is null', 'value': 0, 'operator': 'eq'},
        {'sql': 'select count(*) from time where start_time is null', 'value': 0, 'operator': 'eq'},
        {'sql': 'select count(*) from users where userid is null', 'value': 0, 'operator': 'eq'},
    ]
)

end_operator = DummyOperator(task_id='End_execution', dag=dag)

# Set operator relationships

start_operator \
    >> [stage_events_to_redshift, stage_songs_to_redshift] \
    >> load_songplays_table \
    >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] \
    >> run_quality_checks \
    >> end_operator
