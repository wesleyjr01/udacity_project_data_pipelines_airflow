from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from airflow.providers.postgres.operators.postgres import PostgresOperator
from operators import (
    StageToRedshiftOperator,
    LoadFactOperator,
    LoadDimensionOperator,
    DataQualityOperator,
    DropTablesOperator,
)

from helpers.sql_queries import CreateQueries, CopyFromS3ToRedshift, InsertQueries


default_args = {
    "owner": "udacity",
    "start_date": datetime.today() - timedelta(days=1),
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "email": ["example@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "schedule_interval": "@daily",
}

with DAG(
    "redshift_dw_pipeline",
    default_args=default_args,
    description="Load and transform data in Redshift with Airflow",
) as dag:

    start_operator = DummyOperator(task_id="Begin_execution")

    create_staging_events_table_on_redshift = PostgresOperator(
        task_id="create_staging_events_table",
        postgres_conn_id="redshift",
        sql=CreateQueries.staging_events_table_create,
    )

    create_staging_songs_table_on_redshift = PostgresOperator(
        task_id="create_staging_songs_table",
        postgres_conn_id="redshift",
        sql=CreateQueries.staging_songs_table_create,
    )

    create_time_table_on_redshift = PostgresOperator(
        task_id="create_time_table",
        postgres_conn_id="redshift",
        sql=CreateQueries.time_table_create,
    )

    create_artists_table_on_redshift = PostgresOperator(
        task_id="create_artists_table",
        postgres_conn_id="redshift",
        sql=CreateQueries.artists_table_create,
    )

    create_users_table_on_redshift = PostgresOperator(
        task_id="create_users_table",
        postgres_conn_id="redshift",
        sql=CreateQueries.users_table_create,
    )

    create_songs_table_on_redshift = PostgresOperator(
        task_id="create_songs_table",
        postgres_conn_id="redshift",
        sql=CreateQueries.songs_table_create,
    )

    create_songplays_table_on_redshift = PostgresOperator(
        task_id="create_songplays_table",
        postgres_conn_id="redshift",
        sql=CreateQueries.songplays_table_create,
    )

    copy_events_from_s3_to_redshift = StageToRedshiftOperator(
        task_id="copy_events_to_staging_table",
        redshift_conn_id="redshift",
        aws_conn_id="aws_credentials",
        table="staging_events",
        s3_bucket="udacity-de-files",
        s3_key="raw/logs/",
        sql=CopyFromS3ToRedshift.copy_json_files_sql,
    )

    copy_songs_from_s3_to_redshift = StageToRedshiftOperator(
        task_id="copy_songs_to_staging_table",
        redshift_conn_id="redshift",
        aws_conn_id="aws_credentials",
        table="staging_songs",
        s3_bucket="udacity-de-files",
        s3_key="raw/songs/",
        sql=CopyFromS3ToRedshift.copy_json_files_sql,
    )

    insert_into_time_table = LoadDimensionOperator(
        task_id="insert_into_time_table",
        redshift_conn_id="redshift",
        sql=InsertQueries.time_table_insert,
    )

    insert_into_artists_table = LoadDimensionOperator(
        task_id="insert_into_artists_table",
        redshift_conn_id="redshift",
        sql=InsertQueries.artists_table_insert,
    )

    insert_into_users_table = LoadDimensionOperator(
        task_id="insert_into_users_table",
        redshift_conn_id="redshift",
        sql=InsertQueries.users_table_insert,
    )

    insert_into_songs_table = LoadDimensionOperator(
        task_id="insert_into_songs_table",
        redshift_conn_id="redshift",
        sql=InsertQueries.songs_table_insert,
    )

    insert_into_songplays_table = LoadFactOperator(
        task_id="insert_into_songplays_table",
        redshift_conn_id="redshift",
        sql=InsertQueries.songplays_table_insert,
    )

    # run_quality_checks = DataQualityOperator(task_id="Run_data_quality_checks", dag=dag)

    # drop tables after running the whole DAG
    drop_all_created_tables = DropTablesOperator(
        task_id="drop_all_created_tables",
        redshift_conn_id="redshift",
        tables=[
            "staging_events",
            "staging_songs",
            "songplays",
            "users",
            "songs",
            "artists",
            "time",
        ],
    )

    # end_operator = DummyOperator(task_id="Stop_execution", dag=dag)

    # Group the starting dependencies
    start_operator >> [
        create_staging_events_table_on_redshift,
        create_staging_songs_table_on_redshift,
        create_time_table_on_redshift,
        create_artists_table_on_redshift,
        create_users_table_on_redshift,
        create_songs_table_on_redshift,
        create_songplays_table_on_redshift,
    ]

    # staging tables dependencies
    create_staging_events_table_on_redshift >> copy_events_from_s3_to_redshift
    create_staging_songs_table_on_redshift >> copy_songs_from_s3_to_redshift

    # time table dependencies
    create_time_table_on_redshift >> insert_into_time_table
    copy_events_from_s3_to_redshift >> insert_into_time_table

    # artists table dependencies
    create_artists_table_on_redshift >> insert_into_artists_table
    copy_songs_from_s3_to_redshift >> insert_into_artists_table

    # users table dependencies
    create_users_table_on_redshift >> insert_into_users_table
    copy_events_from_s3_to_redshift >> insert_into_users_table

    # songs table dependencies
    create_artists_table_on_redshift >> create_songs_table_on_redshift
    create_songs_table_on_redshift >> insert_into_songs_table
    copy_songs_from_s3_to_redshift >> insert_into_songs_table

    # songplays table dependencies
    create_users_table_on_redshift >> create_songplays_table_on_redshift
    create_artists_table_on_redshift >> create_songplays_table_on_redshift
    create_songs_table_on_redshift >> create_songplays_table_on_redshift
    create_songplays_table_on_redshift >> insert_into_songplays_table
    copy_events_from_s3_to_redshift >> insert_into_songplays_table
    insert_into_songs_table >> insert_into_songplays_table
    insert_into_artists_table >> insert_into_songplays_table

    # drop all tables
    [
        copy_events_from_s3_to_redshift,
        copy_songs_from_s3_to_redshift,
        insert_into_time_table,
        insert_into_artists_table,
        insert_into_users_table,
        insert_into_songs_table,
        insert_into_songplays_table,
    ] >> drop_all_created_tables
