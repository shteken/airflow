import datetime

import airflow
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.s3_copy_object_operator import S3CopyObjectOperator
from airflow.operators.python_operator import BranchPythonOperator


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
    's3_to_s3',
    default_args=default_args,
    catchup=False,
    schedule_interval='0 9 * * * ' # run every day at 09:00 UTC
)

copy_videos = S3CopyObjectOperator(
    task_id='copy_videos',
    source_bucket_key = 's3://source-data-test/videos/CAvideos.csv',
    dest_bucket_key = 's3://sink-data-test/videos/CAvideos.csv',
    aws_conn_id = 'my_conn_S3',
    dag=dag,
    depends_on_past=False
)

copy_categories = S3CopyObjectOperator(
    task_id='copy_categories',
    source_bucket_key = 's3://source-data-test/categories/CA_category_id.json',
    dest_bucket_key = 's3://sink-data-test/categories/CA_category_id.json',
    aws_conn_id = 'my_conn_S3',
    dag=dag,
    depends_on_past=False
)

[copy_videos, copy_categories]
