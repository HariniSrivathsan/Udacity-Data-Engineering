import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

#sys.path.insert(1, '/home/srivathsan/harini/playground_udacity/data-engineering/playground/input/Data_Pipelines/project/plugins/operators/')

from airflow.operators import StageToRedshiftOperator
from airflow.operators import LoadFactOperator
from airflow.operators import LoadDimensionOperator
from airflow.operators import DataQualityOperator

from helpers import sqlQueries
#import helpers

default_args = {
    'owner': 'fs',
    'start_date': datetime(2020, 5, 20),
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'depends_on_past': False,
    'schedule_interval': '@hourly'
}

dag = DAG(
    'etl_task',
    default_args=default_args,
    description='ETL in Redshift with Airflow'
)

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id="stage_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_events",
    s3_bucket='udacity-dend',
    s3_key="log_data/",
    extra_params="format as json 's3://udacity-dend/log_json_path.json'",
    dag=dag
)

stage_songs_to_redshift = StageToRedshiftOperator(
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_songs",
    s3_bucket='udacity-dend',
    s3_key="song_data",
    extra_params="json 'auto' compupdate off region 'us-west-2'",
    task_id='Stage_songs',
    dag=dag
)

load_songplays_table = LoadFactOperator(
    task_id='load_songplays_fact_table',
    redshift_conn_id="redshift",
    table="songplays",
    sql_source=helpers.SqlQueries.songplay_table_insert,
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='load_user_dim_table',
    redshift_conn_id="redshift",
    table="users",
    sql_source=helpers.SqlQueries.user_table_insert,
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='load_song_dim_table',
    redshift_conn_id="redshift",
    table="songs",
    sql_source=helpers.SqlQueries.song_table_insert,
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='load_artist_dim_table',
    redshift_conn_id="redshift",
    table="artists",
    sql_source=helpers.SqlQueries.artist_table_insert,
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='load_time_dim_table',
    redshift_conn_id="redshift",
    table="time",
    sql_source=helpers.SqlQueries.time_table_insert,
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='run_data_quality_checks',
    redshift_conn_id="redshift",
    table="time",
    dag=dag
)

end_operator = DummyOperator(task_id='stop_execution',  dag=dag)

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator
