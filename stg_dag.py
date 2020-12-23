import datetime
import airflow
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.aws_athena_operator import AWSAthenaOperator
from airflow.operators.s3_file_transform_operator import S3FileTransformOperator
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
    'stg_dag',
    default_args=default_args,
    catchup=False,
    schedule_interval='0 10 * * * ' # run every day at 10:00 UTC
)

drop_id_query = """
SELECT 
  diagnosis,
  radius_mean,
  texture_mean,
  perimeter_mean,
  area_mean,
  smoothness_mean,
  compactness_mean,
  concavity_mean,
  concave_points_mean,
  symmetry_mean,
  fractal_dimension_mean,
  radius_se,
  texture_se,
  perimeter_se,
  area_se,
  smoothness_se,
  compactness_se,
  concavity_se,
  concave_points_se,
  symmetry_se,
  fractal_dimension_se,
  radius_worst,
  texture_worst,
  perimeter_worst,
  area_worst,
  smoothness_worst,
  compactness_worst,
  concavity_worst,
  concave_points_worst,
  symmetry_worst,
  fractal_dimension_worst 
FROM data
"""

drop_id_column = AWSAthenaOperator(
    task_id='drop_id_column',
    aws_conn_id='my_conn_S3',
    query=drop_id_query,
    output_location='s3://sink-data-test/cancer/stg',
    database='cancer',
    dag=dag,
    depends_on_past=False
)

transformer = S3FileTransformOperator(
    task_id='transformer',
    source_s3_key='s3://sink-data-test/cancer/stg/{{ task_instance.xcom_pull(task_ids="drop_id_column") }}.csv',
    dest_s3_key='s3://sink-data-test/cancer/statistics/cancer.csv',
    transform_script='/home/baruch/airflow/dags/transformations/statistics.py',
    source_aws_conn_id='my_conn_S3',
    dest_aws_conn_id='my_conn_S3',
    replace=True,
    dag=dag,
    depends_on_past=False
)

drop_id_column >> transformer