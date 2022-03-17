from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    
    sql_query_format = "INSERT INTO {} {};"

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 table = "",
                 sql_insert_select = "",
                 redshift_conn_id="redshift_conn_id",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.table = table
        self.sql_insert_select = sql_insert_select
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        self.log.info(f"table: {self.table}")
        self.log.info(f"sql: {self.sql_insert_select}")
        self.log.info(f"redshift_conn_id: {self.redshift_conn_id}")

        redshift = PostgresHook(self.redshift_conn_id)
        sql_query_insert = self.sql_query_format.format(self.table, self.sql_insert_select)
        self.log.info(f"insert query: {sql_query_insert}")
        redshift.run(sql_query_insert)
        
        
