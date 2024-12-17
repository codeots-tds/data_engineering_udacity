from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',       # Redshift connection ID
                 table='',                  # Target staging table
                 s3_bucket='',              # S3 bucket
                 s3_key='',                 # S3 key (prefix/path to data)
                 file_format='JSON',        # File format: 'JSON', 'CSV', etc.
                 json_path='auto',          # JSONPath file for JSON data
                 region='us-west-2',        # AWS region
                 aws_iam_role='',           # IAM Role ARN for Redshift
                 *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.file_format = file_format
        self.json_path = json_path
        self.region = region
        self.aws_iam_role = aws_iam_role

    def execute(self, context):
        self.log.info(f"Starting StageToRedshiftOperator for table: {self.table}")

        # Get the Redshift connection
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Render the S3 key to support dynamic file paths
        rendered_s3_key = self.s3_key.format(**context)
        s3_path = f"s3://{self.s3_bucket}/{rendered_s3_key}"
        self.log.info(f"Loading data from S3 path: {s3_path} to Redshift table: {self.table}")

        # Prepare the COPY command
        if self.file_format.upper() == 'JSON':
            copy_query = f"""
                COPY {self.table}
                FROM '{s3_path}'
                IAM_ROLE '{self.aws_iam_role}'
                REGION '{self.region}'
                FORMAT AS JSON '{self.json_path}';
            """
        elif self.file_format.upper() == 'CSV':
            copy_query = f"""
                COPY {self.table}
                FROM '{s3_path}'
                IAM_ROLE '{self.aws_iam_role}'
                REGION '{self.region}'
                FORMAT AS CSV
                IGNOREHEADER 1;
            """
        else:
            raise ValueError(f"Unsupported file format: {self.file_format}")

        # Execute the COPY command
        self.log.info(f"Executing COPY command: {copy_query}")
        redshift.run(copy_query)

        self.log.info(f"StageToRedshiftOperator for table {self.table} completed successfully")