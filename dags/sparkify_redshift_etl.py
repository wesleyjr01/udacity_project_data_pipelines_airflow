from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from airflow.operators.sql import SQLCheckOperator, SQLValueCheckOperator

from airflow.providers.postgres.operators.postgres import PostgresOperator
from operators import (
    StageToRedshiftOperator,
    LoadFactOperator,
    LoadDimensionOperator,
    DropTablesOperator,
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
    "schedule_interval": "@daily",
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

    # Data QualityChecks
    # Staging events has rows?
    staging_events_has_rows = SQLCheckOperator(
        task_id="staging_events_has_rows",
        conn_id="redshift",
        sql="SELECT COUNT(*) FROM staging_events",
    )

    # Staging songs has rows?
    staging_songs_has_rows = SQLCheckOperator(
        task_id="staging_songs_has_rows",
        conn_id="redshift",
        sql="SELECT COUNT(*) FROM staging_songs",
    )

    # Staging events has the right number of rows?
    staging_events_has_correct_number_rows = SQLValueCheckOperator(
        task_id="staging_events_has_correct_number_rows",
        conn_id="redshift",
        sql="SELECT COUNT(*) FROM staging_events",
        pass_value=8056,
        tolerance=0.01,
    )

    # Staging songs has the right number of rows?
    staging_songs_has_correct_number_rows = SQLValueCheckOperator(
        task_id="staging_songs_has_correct_number_rows",
        conn_id="redshift",
        sql="SELECT COUNT(*) FROM staging_songs",
        pass_value=71,
        tolerance=0.01,
    )

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

    # data quality checks staging_events has rows
    copy_events_from_s3_to_redshift >> staging_events_has_rows
    staging_events_has_rows >> insert_into_time_table
    staging_events_has_rows >> insert_into_users_table
    staging_events_has_rows >> insert_into_songplays_table

    # data quality checks staging_event has right number of rows
    copy_events_from_s3_to_redshift >> staging_events_has_correct_number_rows
    staging_events_has_correct_number_rows >> insert_into_time_table
    staging_events_has_correct_number_rows >> insert_into_users_table
    staging_events_has_correct_number_rows >> insert_into_songplays_table

    # data quality check staging_songs has rows
    copy_songs_from_s3_to_redshift >> staging_songs_has_rows
    staging_songs_has_rows >> insert_into_songs_table
    staging_songs_has_rows >> insert_into_artists_table

    # data quality check staging_songs has right number of rows
    copy_songs_from_s3_to_redshift >> staging_songs_has_correct_number_rows
    staging_songs_has_correct_number_rows >> insert_into_songs_table
    staging_songs_has_correct_number_rows >> insert_into_artists_table

    # end dag
    [
        copy_events_from_s3_to_redshift,
        copy_songs_from_s3_to_redshift,
        insert_into_time_table,
        insert_into_artists_table,
        insert_into_users_table,
        insert_into_songs_table,
        insert_into_songplays_table,
    ] >> end_operator

    end_operator >> drop_all_created_tables
