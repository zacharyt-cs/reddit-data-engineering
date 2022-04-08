import os
from datetime import datetime
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator

from custom_scripts.ingest_reddit import extract_reddit_data
from custom_scripts.preprocessing import json_to_csv, csv_to_parquet

BUCKET = os.environ.get("GCP_GCS_BUCKET")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get('BIGQUERY_DATASET', 'stocks_data')
PROJECT_ID = os.environ.get('GCP_PROJECT_ID')

def load_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

def reddit_pipeline_template(
    # arguments
    dag,
    subreddit,
    mode,
    json_filepath,
    csv_filepath,
    parquet_filepath,
    gcs_path
):
    with dag:
        download_data_task = PythonOperator(
            task_id = 'ingest_reddit_json',
            python_callable = extract_reddit_data,
            op_kwargs = {
                'subreddit': subreddit,
                'mode': mode,
                'start': '{{ data_interval_start }}',
                'end': '{{ data_interval_end }}',
                'filepath': json_filepath
            }
        )

        json_to_csv_task = PythonOperator(
            task_id = 'json_to_csv',
            python_callable = json_to_csv,
            op_kwargs = {
                'json_filepath': json_filepath,
                'csv_filepath': csv_filepath,
                'mode': mode
            }
        )

        csv_to_parquet_task = PythonOperator(
            task_id = 'csv_to_parquet',
            python_callable = csv_to_parquet,
            op_kwargs = {
                'csv_filepath': csv_filepath,
                'parquet_filepath': parquet_filepath
            }
        )

        load_to_gcs_task = PythonOperator(
            task_id = "load_to_gcs",
            python_callable = load_to_gcs,
            op_kwargs={
                "bucket": BUCKET,
                "object_name": gcs_path,
                "local_file": parquet_filepath,
            }
        )

        delete_local_json_csv = BashOperator(
            task_id = "delete_local_json_csv",
            bash_command = f'rm {json_filepath} {csv_filepath}'
        )

        create_BQ_external_table_task = BigQueryCreateExternalTableOperator(
            task_id = f'create_external_table_task',
            table_resource={
                "tableReference": {
                    "projectId": PROJECT_ID,
                    "datasetId": BIGQUERY_DATASET,
                    "tableId": f"{subreddit}_{mode}_external_table",
                },
                "externalDataConfiguration": {
                    "autodetect": "True",
                    "sourceFormat": "PARQUET",
                    "sourceUris": f"gs://{BUCKET}/{gcs_path}",
                },
            },
        )
       
        # Create a partitioned table from external table
        bq_create_partitioned_table_task = BigQueryInsertJobOperator(
            task_id = f"bq_create_partitioned_table_task",
            configuration={
                "query": {
                    "query": "{% include 'load_datawarehouse.sql' %}",
                    "useLegacySql": False,
                }
            }
        )

        download_data_task >> json_to_csv_task >> csv_to_parquet_task >> load_to_gcs_task >> [delete_local_json_csv, create_BQ_external_table_task]
        create_BQ_external_table_task >> bq_create_partitioned_table_task

default_args = {
    "owner": "Zachary",
    "start_date": datetime(2022, 2, 1),
    "end_date": datetime(2022, 3, 31),
    "depends_on_past": True,
    "retries": 1,
}

# all dag definitions (dag = DAG()) should be in the global scope
stocks_submission_weekly_dag = DAG(
        dag_id = 'stocks_submission_weekly',
        schedule_interval = '@weekly',
        catchup = True,
        max_active_runs = 2,
        default_args = default_args,
        user_defined_macros={
            "BIGQUERY_DATASET": BIGQUERY_DATASET,
            "subreddit": 'stocks',
            "mode": 'submission',
        }
    )

# submission
reddit_pipeline_template(
    dag = stocks_submission_weekly_dag,
    subreddit = 'stocks',
    mode = 'submission',
    json_filepath = AIRFLOW_HOME + '/data/json/stocks_submission_{{ data_interval_start.strftime("%Y-%m-%d") }}.json',
    csv_filepath = AIRFLOW_HOME + '/data/csv/stocks_submission_{{ data_interval_start.strftime("%Y-%m-%d") }}.csv',
    parquet_filepath = AIRFLOW_HOME + '/data/parquet/stocks_submission_{{ data_interval_start.strftime("%Y-%m-%d") }}.parquet',
    gcs_path = 'stocks/submission/stocks_submission_{{ data_interval_start.strftime("%Y-%m-%d") }}.parquet'
)

stocks_comment_weekly_dag = DAG(
        dag_id = 'stocks_comment_weekly',
        schedule_interval = '@weekly',
        catchup = True,
        max_active_runs = 2,
        default_args = default_args
    )

# comment
reddit_pipeline_template(
    dag = stocks_comment_weekly_dag,
    subreddit = 'stocks',
    mode = 'comment',
    json_filepath = AIRFLOW_HOME + '/data/json/stocks_comment_{{ data_interval_start.strftime("%Y-%m-%d") }}.json',
    csv_filepath = AIRFLOW_HOME + '/data/csv/stocks_comment_{{ data_interval_start.strftime("%Y-%m-%d") }}.csv',
    parquet_filepath = AIRFLOW_HOME + '/data/parquet/stocks_comment_{{ data_interval_start.strftime("%Y-%m-%d") }}.parquet',
    gcs_path = 'stocks/comment/stocks_comment_{{ data_interval_start.strftime("%Y-%m-%d") }}.parquet'
)