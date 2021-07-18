from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from airflow.operators.sql import SQLCheckOperator, SQLValueCheckOperator

from airflow.providers.postgres.operators.postgres import PostgresOperator
from operators import (
    StageToRedshiftOperator,
    LoadFactOperator,
    LoadDimensionOperator,
    DataQualityOperator,
)

from helpers.sql_queries import CreateQueries, InsertQueries


default_args = {
    "owner": "udacity",
    "start_date": datetime.today() - timedelta(days=3),
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "email": ["example@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "schedule_interval": "@hourly",
    "depends_on_past": False,  # tasks can execute if the previous failed
}

with DAG(
    "redshift_dw_pipeline",
    default_args=default_args,
    catchup=False,  # Dont run tasks on backfills, only the latest task
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

    # Redshift COPY command from S3 data using the 'auto' switch is case sensitive for JSON
    copy_events_from_s3_to_redshift = StageToRedshiftOperator(
        task_id="copy_events_to_staging_table",
        redshift_conn_id="redshift",
        aws_conn_id="aws_credentials",
        table="staging_events",
        s3_bucket="udacity-de-files",
        s3_key="raw/logs/",
        sql="""
            COPY {}
            FROM '{}'
            ACCESS_KEY_ID '{}'
            SECRET_ACCESS_KEY '{}'
            FORMAT AS JSON 's3://udacity-de-files/raw/log_json_path.json'
        """,
    )

    copy_songs_from_s3_to_redshift = StageToRedshiftOperator(
        task_id="copy_songs_to_staging_table",
        redshift_conn_id="redshift",
        aws_conn_id="aws_credentials",
        table="staging_songs",
        s3_bucket="udacity-de-files",
        s3_key="raw/songs/",
        sql="""
            COPY {}
            FROM '{}'
            ACCESS_KEY_ID '{}'
            SECRET_ACCESS_KEY '{}'
            FORMAT AS JSON 'auto'
        """,
    )

    insert_into_time_table = LoadDimensionOperator(
        task_id="insert_into_time_table",
        redshift_conn_id="redshift",
        load_mode="delete-reload",
        sql=InsertQueries.time_table_full_load,
        source_table="staging_events",
        target_table="time",
    )

    insert_into_artists_table = LoadDimensionOperator(
        task_id="insert_into_artists_table",
        redshift_conn_id="redshift",
        load_mode="append",
        sql=InsertQueries.artists_table_incremental_load,
        source_table="staging_songs",
        target_table="artists",
    )

    insert_into_users_table = LoadDimensionOperator(
        task_id="insert_into_users_table",
        redshift_conn_id="redshift",
        load_mode="delete-reload",
        sql=InsertQueries.users_table_full_load,
        source_table="staging_events",
        target_table="users",
    )

    insert_into_songs_table = LoadDimensionOperator(
        task_id="insert_into_songs_table",
        redshift_conn_id="redshift",
        load_mode="delete-reload",
        sql=InsertQueries.songs_table_full_load,
        source_table="staging_songs",
        target_table="songs",
    )

    insert_into_songplays_table = LoadFactOperator(
        task_id="insert_into_songplays_table",
        redshift_conn_id="redshift",
        load_mode="append",
        sql=InsertQueries.songplays_table_incremental_load,
        source_table="staging_events",
        target_table="songplays",
    )

    # Data QualityChecks
    dimension_tables_quality_check = DataQualityOperator(
        task_id="dimension_quality_checks",
        conn_id="redshift",
        dq_checks=[
            {
                "check_sql": "SELECT COUNT(*) FROM songs WHERE artist_id IS NULL",
                "expected_result": 0,
            },
            {
                "check_sql": "SELECT COUNT(*) FROM songs WHERE song_id IS NULL",
                "expected_result": 0,
            },
            {
                "check_sql": "SELECT COUNT(*) FROM users WHERE user_id IS NULL",
                "expected_result": 0,
            },
            {
                "check_sql": "SELECT COUNT(*) FROM artists WHERE artist_id IS NULL",
                "expected_result": 0,
            },
        ],
    )
    fact_tables_quality_check = DataQualityOperator(
        task_id="fact_quality_checks",
        conn_id="redshift",
        dq_checks=[
            {
                "check_sql": "SELECT COUNT(*) FROM songplays WHERE songplay_id IS NULL",
                "expected_result": 0,
            },
            {
                "check_sql": "SELECT COUNT(*) FROM songplays WHERE user_id IS NULL",
                "expected_result": 0,
            },
        ],
    )

    # end the dag
    end_operator = DummyOperator(task_id="Stop_execution", dag=dag)

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

    # data quality on dimension tables
    insert_into_time_table >> dimension_tables_quality_check
    insert_into_users_table >> dimension_tables_quality_check
    insert_into_songs_table >> dimension_tables_quality_check
    insert_into_artists_table >> dimension_tables_quality_check

    # data quality on fact tables
    insert_into_songplays_table >> fact_tables_quality_check

    # end dag
    [
        fact_tables_quality_check,
        dimension_tables_quality_check,
    ] >> end_operator
