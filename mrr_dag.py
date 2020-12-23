import datetime

import airflow
from airflow.contrib.operators.s3_copy_object_operator import S3CopyObjectOperator

default_args = {
    'owner': 'Baruch',
    'depends_on_past': False,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'start_date': datetime.datetime(2020, 12, 5)
}

dag = airflow.DAG(
    'mrr_dag',
    default_args=default_args,
    catchup=False,
    schedule_interval='0 9 * * * ' # run every day at 09:00 UTC
)

copy_data = S3CopyObjectOperator(
    task_id='copy_data',
    source_bucket_key = 's3://source-data-test/cancer/data.csv',
    dest_bucket_key = 's3://sink-data-test/cancer/data.csv',
    aws_conn_id = 'my_conn_S3',
    dag=dag,
    depends_on_past=False
)

copy_data
