from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator, DataprocDeleteClusterOperator, DataprocSubmitJobOperator

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 0,
    'start_date': datetime(2024, 11, 16)
}

# Cluster configuration
cluster_config = {
    'project_id': 'swift-terra-440607-n3',
    'cluster_name': 'earthquake-rsd',
    'region': 'us-central1',
    'config': {
        'master_config': {
            'num_instances': 1,
            'machine_type_uri': 'e2-standard-2',
            'disk_config': {'boot_disk_type': 'pd-standard', 'boot_disk_size_gb': 50}  # Adjust disk size here
        },
        'worker_config': {
            'num_instances': 2,
            'machine_type_uri': 'e2-standard-2',
            'disk_config': {'boot_disk_type': 'pd-standard', 'boot_disk_size_gb': 50}  # Adjust disk size here
        },
        'software_config': {
            'image_version': '2.1'
        }
    }
}


# Spark job configuration
SPARK_JOB = {
    "reference": {"project_id": "swift-terra-440607-n3"},
    "placement": {"cluster_name": cluster_config['cluster_name']},
    "pyspark_job": {
        "main_python_file_uri": "gs://earthquake-bucket-rsd/Pyspark_Jobs/Daily_data_loading_using_pyspark.py",
        "jar_file_uris": [
            "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.30.0.jar"
        ],
        'python_file_uris': [
            'gs://earthquake-bucket-rsd/Pyspark_Jobs/Config_pyspark.py',
            'gs://earthquake-bucket-rsd/Pyspark_Jobs/util.py',
        ]
    }
}

# Define the DAG
with DAG(
    "earthquake_data",
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    # Task to create Dataproc cluster
    create_cluster = DataprocCreateClusterOperator(
        task_id='create_dataproc_cluster',
        project_id=cluster_config['project_id'],
        cluster_config=cluster_config['config'],
        region=cluster_config['region'],
        cluster_name=cluster_config['cluster_name'],

    )

    # Task to submit Spark job
    submit_spark_job = DataprocSubmitJobOperator(
        task_id='submit_spark_job',
        job=SPARK_JOB,
        region=cluster_config['region'],
        project_id=cluster_config['project_id'],
        gcp_conn_id='google_cloud_default'
    )

    # Task to delete Dataproc cluster
    delete_cluster = DataprocDeleteClusterOperator(
        task_id='delete_dataproc_cluster',
        cluster_name=cluster_config['cluster_name'],
        project_id=cluster_config['project_id'],
        region=cluster_config['region'],
        trigger_rule='all_done',  # Ensures cluster is deleted even if the job fails
        gcp_conn_id='google_cloud_default'
    )

    # Set task dependencies
    create_cluster >> submit_spark_job >> delete_cluster