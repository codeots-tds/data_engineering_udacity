from airflow.models import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from datetime import datetime

def list_s3_keys(bucket_name, prefix=None, **kwargs):
    """List keys in an S3 bucket."""
    try:
        s3_hook = S3Hook(aws_conn_id="my_aws_conn")  # Use your custom AWS connection
        keys = s3_hook.list_keys(bucket_name=bucket_name, prefix=prefix)
        if keys:
            print(f"Found keys: {keys}")
        else:
            print(f"No keys found in bucket '{bucket_name}' with prefix '{prefix}'.")
        return keys
    except Exception as e:
        print(f"Error listing keys in bucket '{bucket_name}': {e}")
        raise

with DAG(
    dag_id="list_s3_keys_dag",
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    list_keys_task = PythonOperator(
        task_id="list_s3_keys",
        python_callable=list_s3_keys,
        op_kwargs={
            "bucket_name": "sparkify-project-bucket",  # Replace with your bucket name
            "prefix": "my_prefix/",  # Replace with your prefix if needed
        },
    )
