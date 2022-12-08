from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='redshift', 
                 sql='',
                 append_only=False,
                 table='',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql
        self.append_only=append_only
        self.table=table
        

    def execute(self, context):
        self.log.info('setting redshift connection')
        redshift = PostgresHook(self.redshift_conn_id)
        self.log.info(f'append_only is set to {self.append_only}')
        if self.append_only:
            rendered_sql = f'INSERT INTO {self.table} {self.sql}'
            redshift.run(rendered_sql)
        else:
            # truncating first
            self.log.info(f'truncating {self.table} table')
            redshift.run(f'TRUNCATE TABLE {self.table}')
            # inserting
            self.log.info(f'now inserting into {self.table} table')
            redshift.run(f'INSERT INTO {self.table} {self.sql}')
