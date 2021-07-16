from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    ui_color = "#80BD9E"

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id: str,
        source_table: str,
        target_table: str,
        sql: str,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.source_table = source_table
        self.target_table = target_table
        self.sql = sql

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Inserting data into dimension table {self.target_table}.")
        redshift_hook.run(
            self.sql.format(
                source_table=self.source_table,
                target_table=self.target_table,
            )
        )
