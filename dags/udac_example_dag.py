from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

S3_BUCKET='udacity-dend'
S3_KEY_LOG='log_data'
S3_KEY_SONG='song_data'
CONN_ID_REDSHIFT='redshift_conn_id'
CONN_ID_AWS='aws_conn_id'
    
default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'max_active_runs': 1,
    'catchup': False
}

dag = DAG('udac_example_dag5',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          start_date=datetime(2021, 10, 21),
          schedule_interval="@daily"
        )

start_operator = DummyOperator(task_id='Start_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='stage_events_to_redshift',
    dag=dag,
    table="staging_events",
    redshift_conn_id=CONN_ID_REDSHIFT,
    aws_conn_id=CONN_ID_AWS,
    s3_bucket=S3_BUCKET,
    s3_key=S3_KEY_LOG,
    extra_options="FORMAT AS JSON 's3://udacity-dend/log_json_path.json'"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='stage_songs_to_redshift',
    dag=dag,
    table="staging_songs",
    redshift_conn_id=CONN_ID_REDSHIFT,
    aws_conn_id=CONN_ID_AWS,
    s3_bucket=S3_BUCKET,
    s3_key=S3_KEY_SONG,
    extra_options="JSON 'auto' COMPUPDATE OFF"
)

load_songplays_table = LoadFactOperator(
    task_id='load_songplays_table',
    dag=dag,
    table='songplays',
    sql_insert_select=SqlQueries.songplay_table_insert,
    redshift_conn_id=CONN_ID_REDSHIFT
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='load_user_dimension_table',
    dag=dag,
    table="users",
    sql_insert_select = SqlQueries.user_table_insert,
    sql_query_upsert_delete = SqlQueries.users_table_upsert_delete,
    redshift_conn_id=CONN_ID_REDSHIFT
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='load_song_dimension_table',
    dag=dag,
    table="songs",
    sql_insert_select = SqlQueries.song_table_insert,
    sql_query_upsert_delete = SqlQueries.songs_table_upsert_delete,
    redshift_conn_id=CONN_ID_REDSHIFT
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='load_artist_dimension_table',
    dag=dag,
    table="artists",
    sql_insert_select = SqlQueries.artist_table_insert,
    sql_query_upsert_delete = SqlQueries.artists_table_upsert_delete,
    redshift_conn_id=CONN_ID_REDSHIFT
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='load_time_dimension_table',
    dag=dag,
    table="time",
    sql_insert_select = SqlQueries.time_table_insert,
    redshift_conn_id=CONN_ID_REDSHIFT
)

run_quality_checks_songplays = DataQualityOperator(
    task_id='run_quality_checks',
    dag=dag,
    sql_checks=SqlQueries.check_queries_songplays,
    redshift_conn_id=CONN_ID_REDSHIFT
)

run_quality_checks_artists = DataQualityOperator(
    task_id='check_queries_artists',
    dag=dag,
    sql_checks=SqlQueries.check_queries_artists,
    redshift_conn_id=CONN_ID_REDSHIFT
)

run_quality_checks_songs = DataQualityOperator(
    task_id='check_queries_songs',
    dag=dag,
    sql_checks=SqlQueries.check_queries_songs,
    redshift_conn_id=CONN_ID_REDSHIFT
)

run_quality_checks_users = DataQualityOperator(
    task_id='check_queries_users',
    dag=dag,
    sql_checks=SqlQueries.check_queries_users,
    redshift_conn_id=CONN_ID_REDSHIFT
)

run_quality_checks_time = DataQualityOperator(
    task_id='check_queries_time',
    dag=dag,
    sql_checks=SqlQueries.check_queries_time,
    redshift_conn_id=CONN_ID_REDSHIFT
)

end_operator = DummyOperator(task_id='End_execution',  dag=dag)


# the operation to ingest data to stage tables
start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

# the operation to fact and dimention tables
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

stage_events_to_redshift >> load_user_dimension_table

stage_songs_to_redshift >> load_song_dimension_table
stage_songs_to_redshift >> load_artist_dimension_table

load_songplays_table >> load_time_dimension_table

# data quality check
load_songplays_table >> run_quality_checks_songplays
load_user_dimension_table >> run_quality_checks_users
load_song_dimension_table >> run_quality_checks_songs
load_artist_dimension_table >> run_quality_checks_artists
load_time_dimension_table >> run_quality_checks_time

# end
run_quality_checks_songplays >> end_operator
run_quality_checks_users >> end_operator
run_quality_checks_songs >> end_operator
run_quality_checks_artists >> end_operator
run_quality_checks_time >> end_operator
