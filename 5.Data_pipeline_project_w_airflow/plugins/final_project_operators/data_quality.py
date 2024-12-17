from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',   # Redshift connection ID
                 quality_checks=None,   # List of data quality tests (dict format)
                 *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.quality_checks = quality_checks or []  # Default to an empty list if not provided

    def execute(self, context):
        self.log.info('Starting DataQualityOperator')

        # Get the Redshift connection
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Executes each test and validate results
        for check in self.quality_checks:
            sql = check['check_sql']
            expected_result = check['expected_result']
            self.log.info(f"Running test: {sql}")
            records = redshift.get_records(sql)

            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality test failed. No result returned for query: {sql}")
            actual_result = records[0][0]

            # Handle conditional checks
            if isinstance(expected_result, str) and expected_result.startswith('>'):
                threshold = int(expected_result[1:])
                if actual_result <= threshold:
                    raise ValueError(
                        f"Data quality test failed. Expected > {threshold}, Got: {actual_result} for query: {sql}")
            else:
                if actual_result != expected_result:
                    raise ValueError(
                        f"Data quality test failed. Expected: {expected_result}, Got: {actual_result} for query: {sql}")

            self.log.info(f"Test passed. Query: {sql} | Result: {actual_result}")
        self.log.info('All data quality checks passed successfully')
