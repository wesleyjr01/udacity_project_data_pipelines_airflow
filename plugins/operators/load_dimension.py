from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from helpers.sql_queries import DeleteRowsQueries


class LoadDimensionOperator(BaseOperator):
    ui_color = "#80BD9E"

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id: str,
        source_table: str,
        target_table: str,
        load_mode: str,
        sql: str,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.source_table = source_table
        self.target_table = target_table
        self.load_mode = load_mode
        self.sql = sql

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.load_mode == "append":
            self.log.info(f"Incremental load into Dimension table {self.target_table}.")
            redshift_hook.run(
                self.sql.format(
                    source_table=self.source_table,
                    target_table=self.target_table,
                )
            )
        elif self.load_mode == "delete-reload":
            self.log.info(f"Deleting rows from dimension table {self.target_table}.")
            redshift_hook.run(
                DeleteRowsQueries.delete_table_rows.format(table_name=self.target_table)
            )
            self.log.info(f"Full-load into dimension table {self.target_table}.")
            redshift_hook.run(
                self.sql.format(
                    source_table=self.source_table,
                    target_table=self.target_table,
                )
            )
        else:
            raise ValueError(
                "Invalid load_mode, load_mode must be either 'append' or 'delete-reload'"
            )
