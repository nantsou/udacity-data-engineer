from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

default_args = {
    'owner': 'nantsou.liu',
    'start_date': datetime.now(),
    'depends_on_past': False,
    'catchup': False,
    'email_on_failure': False,
    'email_on_retry': False
}

dag = DAG('capstone',
          description='Visualizing the structure of the ETL pipeline of capstone project.',
          default_args=default_args,
          schedule_interval='@once'
          )

start_operator = DummyOperator(task_id='START', dag=dag)
end_operator = DummyOperator(task_id='END', dag=dag)

# load data
load_demographics_operator = DummyOperator(task_id='Load_Demographics_Dataset', dag=dag)
load_airports_operator = DummyOperator(task_id='Load_Airports_Dataset', dag=dag)
load_countries_operator = DummyOperator(task_id='Load_Countries_Dataset', dag=dag)
load_temperature_operator = DummyOperator(task_id='Load_Temperature_Dataset', dag=dag)
load_immigration_operator = DummyOperator(task_id='Load_Immigration_Dataset', dag=dag)

# clean data
clean_demographics_operator = DummyOperator(task_id='Clean_Demographics_Dataset', dag=dag)
clean_airports_operator = DummyOperator(task_id='Clean_Airports_Dataset', dag=dag)
clean_countries_operator = DummyOperator(task_id='Clean_Countries_Dataset', dag=dag)
clean_temperature_operator = DummyOperator(task_id='Clean_Temperature_Dataset', dag=dag)
clean_immigration_operator = DummyOperator(task_id='Clean_Immigration_Dataset', dag=dag)

# transform data
transform_demographics_operator = DummyOperator(task_id='Transform_Demographics_Dataset', dag=dag)
transform_countries_operator = DummyOperator(task_id='Transform_Countries_Dataset', dag=dag)
transform_temperature_operator = DummyOperator(task_id='Transform_Temperature_Dataset', dag=dag)
transform_immigration_operator = DummyOperator(task_id='Transform_Immigration_Dataset', dag=dag)

# model data
model_dim_demographics_operator = DummyOperator(task_id='Model_Dimension_Demographics_Table', dag=dag)
model_dim_airports_operator = DummyOperator(task_id='Model_Dimension_Airports_Dataset', dag=dag)
model_dim_countries_operator = DummyOperator(task_id='Model_Dimension_Countries_Dataset', dag=dag)
model_fact_immigration_operator = DummyOperator(task_id='Model_Fact_Immigration_Table', dag=dag)

# data quality check
quality_check_operator = DummyOperator(task_id='Data_Quality_Checks', dag=dag)

# The relationship of the pipeline
start_operator >> [load_demographics_operator, load_airports_operator, load_countries_operator,
                   load_temperature_operator, load_immigration_operator]

load_demographics_operator >> clean_demographics_operator >> transform_demographics_operator >> model_dim_demographics_operator
load_airports_operator >> clean_airports_operator >> model_dim_airports_operator
load_countries_operator >> clean_countries_operator >> transform_countries_operator >> model_dim_countries_operator
load_temperature_operator >> clean_temperature_operator >> transform_temperature_operator >> model_dim_countries_operator
load_immigration_operator >> clean_immigration_operator >> transform_immigration_operator >> model_fact_immigration_operator

[model_dim_demographics_operator, model_dim_airports_operator, model_dim_countries_operator,
 model_fact_immigration_operator] >> quality_check_operator >> end_operator
