from airflow import DAG
# from airflow.providers.amazon.aws.hooks.redshift import RedshiftSQLHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
import pendulum


def test_redshift_connection():
    """Test the connection to the Redshift cluster."""
    try:
        hook = PostgresHook(postgres_conn_id='my_redshift_conn')  # Use your Redshift connection ID
        conn = hook.get_conn()  # Attempt to connect
        cursor = conn.cursor()
        cursor.execute("SELECT current_database();")  # Simple query to test the connection
        result = cursor.fetchone()
        print(f"Connected to Redshift database: {result[0]}")
    except Exception as e:
        print(f"Error connecting to Redshift: {e}")
        raise

default_args = {
    'owner': 'airflow',
    'start_date': pendulum.datetime(2023, 12, 1, tz="UTC"),
    'retries': 2,
    'retry_delay': pendulum.duration(minutes=5),
}

dag = DAG(
    'test_redshift_connection',
    default_args=default_args,
    description='Test connection to Redshift',
    schedule_interval=None,  # Run manually
    catchup=False,
)

test_connection_task = PythonOperator(
    task_id='test_connection',
    python_callable=test_redshift_connection,
    dag=dag,
)