from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    sql_query_format = "INSERT INTO {} {};"

    @apply_defaults
    def __init__(self,
                 table = "",
                 sql_insert_select = "",
                 sql_query_upsert_delete = "",
                 redshift_conn_id="redshift_conn_id",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.table = table
        self.sql_insert_select = sql_insert_select
        self.sql_query_upsert_delete = sql_query_upsert_delete
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        self.log.info(f"self.table: {self.table}")
        self.log.info(f"self.sql_insert_select: {self.sql_insert_select}")
        self.log.info(f"self.redshift_conn_id: {self.redshift_conn_id}")

        redshift = PostgresHook(self.redshift_conn_id)

        # delete old duplicated data
        if self.sql_query_upsert_delete:
            self.log.info(f"sql_query_upsert_delete: {self.sql_query_upsert_delete}")
            redshift.run(self.sql_query_upsert_delete)
        
        # insert
        sql_query_upsert_insert = self.sql_query_format.format(self.table, self.sql_insert_select)
        self.log.info(f"sql_query_upsert_insert: {sql_query_upsert_insert}")
        redshift.run(sql_query_upsert_insert)
        
        
        