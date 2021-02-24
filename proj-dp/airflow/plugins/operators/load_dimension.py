from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'
    insert_query_template = """
    insert into {}
    {};
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 table,
                 sql,
                 append_only=False,
                 *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        self.append_only = append_only

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if not self.append_only:
            self.log.info(f'Clear the dimension table, {self.table}.')
            redshift.run(f'delete from {self.table}')

        self.log.info(f'Insert data from staging tables into fact table: {self.table}.')
        formatted_query = self.insert_query_template.format(
            self.table,
            self.sql
        )
        redshift.run(formatted_query)
        self.log.info(f'Finish inserting data from staging tables into fact table: {self.table}.')
