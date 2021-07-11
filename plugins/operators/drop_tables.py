from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from typing import List


class DropTablesOperator(BaseOperator):
    ui_color = "#80BD9E"

    @apply_defaults
    def __init__(self, redshift_conn_id: str, tables: List[str], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Dropping tables {self.tables}")
        for table in self.tables:
            redshift_hook.run(f"DROP TABLE IF EXISTS {table} cascade")
