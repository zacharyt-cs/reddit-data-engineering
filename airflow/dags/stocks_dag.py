import os
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.dataproc import ClusterGenerator, DataprocCreateClusterOperator, DataprocSubmitJobOperator

from custom_scripts.ingest_reddit import extract_reddit_data
from custom_scripts.preprocessing import json_to_csv, csv_to_parquet

BUCKET = os.environ.get("GCP_GCS_BUCKET")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get('BIGQUERY_DATASET', 'stocks_data')
PROJECT_ID = os.environ.get('GCP_PROJECT_ID')
PYSPARK_URI = f'gs://{BUCKET}/scripts/wordcount_by_date.py'

CLUSTER_GENERATOR_CONFIG = ClusterGenerator(
            project_id=PROJECT_ID,
            service_account='de-r-stocks-service@de-r-stocks.iam.gserviceaccount.com',
            zone="asia-southeast1-a",
            master_machine_type="n1-standard-4",
            num_masters=1,
            max_idle="900s",
            init_actions_uris=[f'gs://{BUCKET}/scripts/pip-install.sh'],
            metadata={'PIP_PACKAGES': 'spark-nlp'},
        ).make()

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

        QUERY = f'''CREATE OR REPLACE EXTERNAL TABLE {BIGQUERY_DATASET}.{subreddit}_{mode}_external_table
                    OPTIONS (
                        format="PARQUET",
                        uris=["gs://{BUCKET}/{gcs_path}"]
                    );'''

        create_BQ_external_table_task = BigQueryInsertJobOperator(
            task_id = 'create_external_table',
            configuration={
                'query': {
                    'query': QUERY,
                    'useLegacySql': False,
                }
            }
        )
       
        # Create a partitioned table from external table
        BQ_create_partitioned_table_task = BigQueryInsertJobOperator(
            task_id = "bq_create_partitioned_table",
            configuration={
                "query": {
                    "query": "{% include 'load_datawarehouse.sql' %}",
                    "useLegacySql": False,
                }
            }
        )

        create_cluster_operator_task = DataprocCreateClusterOperator(
            task_id='create_dataproc_cluster',
            cluster_name="de-spark-cluster",
            project_id=PROJECT_ID,
            region="asia-southeast1",
            cluster_config=CLUSTER_GENERATOR_CONFIG,
            use_if_exists=True,
            retries=0
        )

        QUERY_CREATE_WORDCOUNT_TABLE = '''
            CREATE TABLE IF NOT EXISTS {{ BIGQUERY_DATASET }}.{{ subreddit }}_{{ mode }}_wordcount (
                word STRING,
                wordcount INTEGER,
                {{ mode }}_date DATE
            )
            PARTITION BY {{ mode }}_date'''
        
        create_wordcount_table_task = BigQueryInsertJobOperator(
            task_id = 'create_wordcount_table',
            configuration={
                'query': {
                    'query': QUERY_CREATE_WORDCOUNT_TABLE,
                    'useLegacySql': False,
                }
            }
        )

        QUERY_DELETE_WORDCOUNT_ROWS = '''
        DELETE {{ BIGQUERY_DATASET }}.{{ subreddit }}_{{ mode }}_wordcount
        WHERE {{ mode }}_date BETWEEN '{{ ds }}' AND '{{ macros.ds_add(data_interval_end.strftime('%Y-%m-%d'), -1) }}';
        '''

        # delete any existing duplicate rows before writing
        delete_wordcountdup_task = BigQueryInsertJobOperator(
            task_id = 'delete_wordcountdup',
            configuration={
                'query': {
                    'query': QUERY_DELETE_WORDCOUNT_ROWS,
                    'useLegacySql': False,
                }
            }
        )
        
        pyspark_job = {
            "reference": {"project_id": PROJECT_ID},
            "placement": {"cluster_name": 'de-spark-cluster'},
            "pyspark_job": {
                "main_python_file_uri": PYSPARK_URI,
                "jar_file_uris": ["gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"],
                "properties": {
                    "spark.jars.packages":"com.johnsnowlabs.nlp:spark-nlp_2.12:3.4.3"
                },
                "args": [
                    f"--input=gs://{BUCKET}/{gcs_path}",
                    f"--dataset={BIGQUERY_DATASET}",
                    f"--subreddit={subreddit}",
                    f"--mode={mode}"
                    ]
            }
        }

        wordcount_sparksubmit_task = DataprocSubmitJobOperator(
            task_id='wordcount_sparksubmit',
            job=pyspark_job,
            region='asia-southeast1',
            project_id=PROJECT_ID,
            trigger_rule='all_done'
        )

        download_data_task >> json_to_csv_task >> csv_to_parquet_task >> load_to_gcs_task >> [delete_local_json_csv, create_BQ_external_table_task]
        create_BQ_external_table_task >> BQ_create_partitioned_table_task >> create_wordcount_table_task
        create_wordcount_table_task >> delete_wordcountdup_task >> create_cluster_operator_task >> wordcount_sparksubmit_task

default_args = {
    "owner": "Zachary",
    "start_date": datetime(2022, 3, 1),
    "end_date": datetime(2022, 4, 30),
    "depends_on_past": False,
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