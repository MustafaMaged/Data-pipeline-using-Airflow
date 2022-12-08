from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                redshift_conn_id='redshift',
                tables=[],
                dq_checks={},
                *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables
        self.dq_checks = dq_checks.get('query_checks')

    def execute(self, context):
        self.log.info('setting redshift connection')
        redshift = PostgresHook(self.redshift_conn_id)
        self.log.info('starting quality checks')
        # first we check if tables were loaded successfully
        self.log.info('checking that tables loaded successfully with at least one row per table')
        for table in self.tables:
            records = redshift.get_records(f'SELECT count(*) FROM {table}')
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed, {table} returned no results")
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality check failed, {table} has 0 rows")
            self.log.info(f"Data quality check on table {table} passed with {records[0][0]} records")
        self.log.info("All tables have at least one row")
        # now let's check for null values in our tables
        
        
        for check in self.dq_checks:
            records = redshift.get_records(check['check_sql'])[0]
            expected_result = check.get('expected_result')
            if records == expected_result:
                raise ValueError(f'null check failed: {check.get("check_sql")}')
        self.log.info(f'null check passed: no null values in primary keys of tables')
        
        
        
        
        
        
        
        