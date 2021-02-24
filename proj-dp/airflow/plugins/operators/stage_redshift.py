from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    copy_query_template = """
        copy {}
        from '{}'
        access_key_id '{}'
        secret_access_key '{}'
        json '{}'
        region '{}';
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 aws_credentials_id,
                 table,
                 s3_path,
                 s3_region,
                 json_path,
                 *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_path = s3_path
        self.s3_region = s3_region
        self.json_path = json_path

    def execute(self, context):
        aws_hook = AwsHook(aws_conn_id=self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f'Clear data in the staging table: {self.table}.')
        redshift.run(f'delete from {self.table}')

        self.log.info(f'Copy the data from S3 into staging table: {self.table}')
        formatted_query = self.copy_query_template.format(
            self.table,
            self.s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.json_path,
            self.s3_region
        )
        redshift.run(formatted_query)
        self.log.info(f'Finish copying the data from S3 into staging table: {self.table}')
