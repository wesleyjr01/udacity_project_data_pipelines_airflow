from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = "#358140"

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id: str,
        aws_conn_id: str,
        table: str,
        s3_bucket: str,
        s3_key: str,
        sql: str,
        delimiter=",",
        ignore_headers=1,
        *args,
        **kwargs,
    ):

        super().__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_conn_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.sql = sql
        self.delimiter = delimiter
        self.ignore_headers = ignore_headers

    def execute(self, context):
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
        credentials = s3_hook.get_credentials()
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Copying data from S3 to staging table {self.table} in Redshift")
        s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
        formatted_sql = self.sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
        )
        redshift_hook.run(formatted_sql)
