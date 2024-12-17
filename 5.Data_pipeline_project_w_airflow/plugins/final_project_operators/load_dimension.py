from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',  # Redshift connection ID
                 table='',             # Target dimension table
                 sql_query='',         # SQL query to load data
                 insert_mode='append',  # 'append' or 'truncate-insert'
                 *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Initialize parameters
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query
        self.insert_mode = insert_mode

    def execute(self, context):
        self.log.info(f"Starting LoadDimensionOperator for table: {self.table}")

        # Get the Redshift connection
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Truncate the table if in 'truncate-insert' mode
        if self.insert_mode == 'truncate-insert':
            self.log.info(f"Truncating table {self.table}")
            truncate_query = f"TRUNCATE TABLE {self.table}"
            redshift.run(truncate_query)

        # Insert data into the dimension table
        self.log.info(f"Inserting data into {self.table}")
        formatted_query = f"INSERT INTO {self.table} {self.sql_query}"
        self.log.info(f"Executing SQL: {formatted_query}")
        redshift.run(formatted_query)

        self.log.info(f"LoadDimensionOperator for table {self.table} completed successfully")
