from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                # Defining operators params
                redshift_conn_id='', #Redshift connection ID
                table='',            #Target dimension table
                sql_query='',        #SQL query to load data
                insert_mode='append',# 'append or truncate'
                *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.table=table
        self.sql_query=sql_query
        self.insert_mode=insert_mode


    def execute(self, context):
        self.log.info(f"Starting LoadFactOperator for table: {self.table}")

        # Get the Redshift connection
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Truncate the table if in 'truncate-insert' mode
        if self.insert_mode == 'truncate-insert':
            self.log.info(f"Truncating table {self.table}")
            truncate_query = f"TRUNCATE TABLE {self.table}"
            redshift.run(truncate_query)

         # Inserting data into the fact table
        self.log.info(f"Inserting data into {self.table}")
        formatted_query = f"INSERT INTO {self.table} {self.sql_query}"
        redshift.run(formatted_query)

        self.log.info(f"LoadFactOperator for table {self.table} completed successfully")
