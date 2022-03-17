from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 sql_checks = [],
                 redshift_conn_id="redshift_conn_id",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.sql_checks = sql_checks
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)
        test_result = True
        for sql_check in self.sql_checks:
            check_sql = sql_check.get('check_sql')
            expected_result = sql_check.get('expected_result')
            self.log.info(f"sql_check.check_sql: {check_sql}")
            self.log.info(f"sql_check.expected_result: {expected_result}")
            
            result = redshift.get_records(check_sql)[0]
            if expected_result != result[0]:
                self.log.warn(f"Test failed: SQL: {check_sql}, ExpectedResult: {expected_result}, Result: {result}")
                test_result = False

        if test_result:
            self.log.info("Test Succeed")
        else:
            raise ValueError('Test Failed')

#       
        