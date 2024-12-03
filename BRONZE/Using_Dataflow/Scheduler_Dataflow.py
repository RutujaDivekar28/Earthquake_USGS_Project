# from airflow import DAG
# from airflow.providers.google.cloud.operators.dataflow import DataflowCreatePythonJobOperator
# from airflow.utils.dates import days_ago
#
# # Default arguments for the DAG
# default_args = {
#     'owner': 'airflow',
#     'retries': 0,
#     'start_date': days_ago(0)
# }
#
# #  configuration
# config = {
#     'project_id': 'swift-terra-440607-n3',
#     'region': 'us-central1'
# }
#
# # Define the DAG
# with DAG(
#     'dataflowdag',
#     default_args=default_args,
#     schedule_interval='@daily',
# ) as dag:
#
#     # Use DataflowCreatePythonJobOperator to create a Dataflow job
#     start_dataflow_job = DataflowCreatePythonJobOperator(
#         task_id='start_dataflow_job',
#         py_file='gs://earthquake-bucket-rsd/Dataflow_Jobs/Daily_data_load_using_dataflow.py',
#         job_name='my-dataflow-job',
#         project_id=config['project_id'],
#         location=config['region'],
#         temp_gcs_location='gs://earthquake-bucket-rsd/temp/',  # Correct argument name
#         gcp_conn_id='google_cloud_default'
#     )
#

from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.dataflow import DataflowCreatePythonJobOperator

# DAG settings
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 11, 16),
    'retries': 0,
}

# Define the DAG with a daily schedule
with DAG(
    dag_id='earthquake_data_dataflow',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # Define the Dataflow job with additional requirements
    dataflow_job = DataflowCreatePythonJobOperator(
        task_id='daily_dataflow_job',
        py_file='gs://earthquake-bucket-rsd/Dataflow_Jobs/Daily_data_load_using_dataflow.py',
        # Dataflow job configuration
        dataflow_default_options={
            'project': 'swift-terra-440607-n3',
            'region': 'us-central1',
            'temp_location': 'gs://earthquake-bucket-rsd/temp',
            'staging_location': "gs://earthquake-bucket-rsd/temp",
            'runner': 'DirectRunner',
        },
        gcp_conn_id='google_cloud_default'
    )

    dataflow_job