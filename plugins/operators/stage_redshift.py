from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.base_hook import BaseHook
# from airflow.providers.postgres.operators.postgres import PostgresOperator

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    # https://docs.aws.amazon.com/redshift/latest/dg/copy-parameters-data-source-s3.html
#     sql_query_test = "SELECT * FROM venue LIMIT 10;"
    sql_query = """
    COPY {} FROM '{}' 
    ACCESS_KEY_ID '{}' 
    SECRET_ACCESS_KEY '{}' 
    REGION '{}' 
    {};"""
    
    

    @apply_defaults
    def __init__(self,
                 table="",
                 redshift_conn_id="",
                 aws_conn_id="",
                 s3_bucket="",
                 s3_key="",
                 extra_options="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.extra_options = extra_options

    def execute(self, context):
        self.log.info(f"redshift_conn_id: {self.redshift_conn_id}")
        self.log.info(f"aws_conn_id: {self.aws_conn_id}")
        self.log.info(f"s3_bucket: {self.s3_bucket}")
        self.log.info(f"s3_key: {self.s3_key}")
        self.log.info(f"extra_options: {self.extra_options}")

        conn = BaseHook.get_connection(self.aws_conn_id)
        extra = conn.extra_dejson
        region = extra.get('region_name')
        s3_path = f"s3://{self.s3_bucket}/{self.s3_key}"
        self.log.info(f"region: {region}")
        self.log.info(f"conn.login: {conn.login}")
        self.log.info(f"conn.password: {conn.password}")
        self.log.info(f"s3_path: {s3_path}")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        query = self.sql_query.format(self.table,
            s3_path,
            conn.login,
            conn.password,
            region,
            self.extra_options)
        self.log.info(f"query: {query}")
        redshift.run(query)
#         aws_hook = AwsHook(self.aws_conn_id);
#         aws_credentials = aws_hook.get_credentials()
#         aws_credentials = aws_hook.get_credentials()
#         self.log.info(f"secret_key: {aws_credentials.secret_key}")
#         self.log.info(f"access_key: {aws_credentials.access_key}")
        
        
        





