from plugins.final_project_operators.data_quality import DataQualityOperator
from plugins.final_project_operators.load_dimension import LoadDimensionOperator
from plugins.final_project_operators.load_fact import LoadFactOperator
from plugins.final_project_operators.stage_redshift import StageToRedshiftOperator
from sql_queries import final_project_sql_statements

# print(DataQualityOperator)

import pendulum
from datetime import timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator


# Default DAG arguments/parameters
default_args = {
    'owner': 'udacity',
    'start_date': pendulum.datetime(2023, 11, 16, tz="UTC"),  # Set the start date
    'depends_on_past': False, #depends on success/failure of same task in previous execution
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False, #backfill period to schedule dag runs between start date and current date
    'email_on_retry': False
}

# Define the DAG
dag = DAG(
    'sparkify_final_dag', #Need to leanr better naming conventions
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@hourly'
)

# Task Definitions
start_operator = EmptyOperator(task_id='Begin_execution', dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table='staging_events',
    redshift_conn_id='my_redshift_conn',
    aws_iam_role='arn:aws:iam::539247463722:role/sparkify_project_role',
    s3_bucket='sparkify-project-bucket',
    s3_key='log_data/',
    json_path='s3://sparkify-project-bucket/log_json_path.json',
    region='us-east-1'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table='staging_songs',
    redshift_conn_id='my_redshift_conn',
    aws_iam_role='arn:aws:iam::539247463722:role/sparkify_project_role',
    s3_bucket='sparkify-project-bucket',
    s3_key='song_data/',
    json_path='auto',
    region='us-east-1'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table='songplays',
    redshift_conn_id='my_redshift_conn',
    sql_query=final_project_sql_statements.SqlQueries.songplay_table_insert,
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table='users',
    redshift_conn_id='my_redshift_conn',
    sql_query=final_project_sql_statements.SqlQueries.user_table_insert,
    insert_mode='truncate-insert'
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table='songs',
    redshift_conn_id='my_redshift_conn',
    sql_query=final_project_sql_statements.SqlQueries.song_table_insert,
    insert_mode='truncate-insert'
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table='artists',
    redshift_conn_id='my_redshift_conn',
    sql_query=final_project_sql_statements.SqlQueries.artist_table_insert,
    insert_mode='truncate-insert'
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table='time',
    redshift_conn_id='my_redshift_conn',
    sql_query=final_project_sql_statements.SqlQueries.time_table_insert,
    insert_mode='truncate-insert'
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='my_redshift_conn',
    quality_checks=[
        {'check_sql': "SELECT COUNT(*) FROM users WHERE user_id IS NULL", 'expected_result': 0},
        {'check_sql': "SELECT COUNT(*) FROM songplays", 'expected_result': '> 0'}
    ]
)

end_operator = EmptyOperator(task_id='Stop_execution', dag=dag)

# Task Dependencies
start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]
[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table]
[load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks
run_quality_checks >> end_operator
