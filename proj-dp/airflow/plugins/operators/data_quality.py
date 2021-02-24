from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'
    operators = {
        'eq': lambda x, y: x == y,
        'ne': lambda x, y: x != y,
        'ge': lambda x, y: x >= y,
        'le': lambda x, y: x <= y,
        'gt': lambda x, y: x > y,
        'lt': lambda x, y: x < y,
    }

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 dq_checks,
                 *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dq_checks = dq_checks

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        errors = []

        for dq_check in self.dq_checks:
            records = redshift.get_records(dq_check['sql'])
            if len(records) < 1 or len(records[0]) < 1:
                # The data type returned by redshift.get_records similar to List[Tuple]
                errors.append(f'No records found with {dq_check["sql"]}')
            else:
                if self.operators[dq_check['operator']](records[0][0], dq_check['value']):
                    self.log.info(f'Data quality check on {dq_check["sql"]} passed.')
                else:
                    errors.append(f'Data quality check on {dq_check["sql"]} failed.')

        if errors:
            raise ValueError('\n'.join(errors))
        else:
            self.log.info('Data quality check finished without any errors.')
