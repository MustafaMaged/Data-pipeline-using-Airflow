from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    

    ui_color = '#F98866'
    insert_stmt = ''' INSERT INTO {} 
                      {}
        '''
    @apply_defaults
    def __init__(self,
                 redshift_conn_id='redshift', 
                 sql='',
                 table='',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql
        self.table = table

    def execute(self, context):
        self.log.info('setting redshift connection')
        redshift = PostgresHook(self.redshift_conn_id)
        self.log.info('creating song_play table')
        redshift.run(LoadFactOperator.insert_stmt.format(self.table,self.sql))
