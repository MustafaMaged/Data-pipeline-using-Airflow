from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

import logging

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_sql = '''
    COPY {}
    FROM '{}'
    ACCESS_KEY_ID '{}'
    SECRET_ACCESS_KEY '{}'
    format as json {}  
    '''

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='redshift',
                 aws_credentials_id='aws_credentials',
                 s3_bucket='udacity-dend',
                 s3_key='', 
                 table='',
                 json_paths="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id= aws_credentials_id
        self.s3_bucket=s3_bucket
        self.s3_key=s3_key
        self.table=table
        self.json_paths=json_paths
        

    def execute(self, context):
        self.log.info('start execution')
        # get our connection hooks
        aws_hook = AwsHook(self.aws_credentials_id)
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        # get credentials
        credentials= aws_hook.get_credentials()
        
        # cleaning and loading data
        logging.info('clearing records from destination redshift table')
        redshift.run(f'DELETE FROM {self.table}')
        logging.info('configuring json path')
        if self.json_paths == "":
            s3_json_path = "\'auto\'"
        else:
            s3_json_path = "\'s3://{}/{}\'".format(self.s3_bucket, \
            self.json_paths)
        logging.info(f'json path -> {s3_json_path}')
        logging.info('loading staging table from S3 to Redshift')
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        rendered_sql = StageToRedshiftOperator.copy_sql.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key,
                s3_json_path     
        )
        logging.info(rendered_sql)
        redshift.run(rendered_sql)
        





