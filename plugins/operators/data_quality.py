from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException

from typing import List, Dict


class DataQualityOperator(BaseOperator):
    ui_color = "#89DA59"

    @apply_defaults
    def __init__(self, conn_id: str, dq_checks: List[Dict], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.dq_checks = dq_checks

    def execute(self, context):
        db_hook = PostgresHook(postgres_conn_id=self.conn_id)

        for check in self.dq_checks:
            check_sql = check.get("check_sql")
            expected_result = check.get("expected_result")
            self.log.info(f"Check: {check}")
            if check_sql is None:
                raise AirflowException(
                    "'check_sql' key is missing for each dictionary provided on dq_checks."
                )
            elif expected_result is None:
                raise AirflowException(
                    "'expected_result' key is missing for each dictionary provided on dq_checks."
                )
            else:
                self.log.info(f"Executing SQL check: {check_sql}")
                records = db_hook.get_first(check_sql)[0]
                self.log.info(f"Records returned: {records}")
                if not expected_result == records:
                    raise AirflowException(
                        f"Expected Result is {expected_result}, but check_sql returned {records}"
                    )
                else:
                    self.log.info(f"Check passed.")
