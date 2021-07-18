from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    ui_color = "#F98866"

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
            self.log.info(f"Incremental load into Fact table {self.target_table}.")
            redshift_hook.run(
                self.sql.format(
                    source_table=self.source_table,
                    target_table=self.target_table,
                )
            )
        else:
            raise ValueError(
                "Invalid load_mode. For Fact tables, only load_mode='append' is allowed"
            )
