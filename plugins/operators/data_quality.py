from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException


class DataQualityOperatorHasRows(BaseOperator):
    ui_color = "#89DA59"

    @apply_defaults
    def __init__(self, redshift_conn_id: str, sql: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql

    def execute(self, context):
        self.log.info("Executing SQL check: %s", self.sql)
        records = self.get_db_hook().get_first(self.sql)

        self.log.info("Record: %s", records)
        if not records:
            raise AirflowException("The query returned None")
        elif not all(bool(r) for r in records):
            raise AirflowException(
                f"Test failed.\nQuery:\n{self.sql}\nResults:\n{records!s}"
            )

        self.log.info("Success.")
