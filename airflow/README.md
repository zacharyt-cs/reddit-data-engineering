# Documentation

## File structure

### custom_scripts
This directory contains Python modules that are called by the PythonOperator in Airflow.

### dags
This directory contains the DAG scripts, as well as SQL code that is called by the BigQueryInsertJobOperator in Airflow.

## Code breakdown

### Call Python functions from custom modules
Official doc: https://airflow.apache.org/docs/apache-airflow/stable/modules_management.html

- In DAG script (`stocks_dag.py`), import module:
`from custom_scripts.ingest_reddit import extract_reddit_data`
This allows you to set `extract_reddit_data` in python_callable of PythonOperator.

- In `Dockerfile`, set env:
`ENV PYTHONPATH="/opt/airflow/custom_scripts:${PYTHONPATH}"`

- In `docker-compose.yml`, mount directories under `x-airflow-common/volumes` so that Airflow can 'see' these paths:
```
volumes:
  - ./custom_scripts:/opt/airflow/custom_scripts
  - ./data/json:/opt/airflow/data/json
  - ./data/csv:/opt/airflow/data/csv
  - ./data/parquet:/opt/airflow/data/parquet
```

### Create a Dataproc cluster in Airflow
Official doc: https://airflow.apache.org/docs/apache-airflow-providers-google/stable/operators/cloud/dataproc.html#examples-of-job-configurations-to-submit

The `gcloud` bash command is as follows:
```
gcloud dataproc clusters create de-spark-cluster \
    --region asia-southeast1 \
    --zone asia-southeast1-a \
    --single-node \
    --master-machine-type n1-standard-4 \
    --master-boot-disk-size 500 \
    --image-version 2.0-debian10 \
    --max-idle 900s \
    --project de-r-stocks \
    --metadata 'PIP_PACKAGES=spark-nlp' \
    --initialization-actions gs://datalake_de-r-stocks/pip-install.sh
```
We can use ClusterGenerator to generate the cluster configuration instead of manually setting the API.
```
from airflow.providers.google.cloud.operators.dataproc import ClusterGenerator, DataprocCreateClusterOperator

CLUSTER_GENERATOR_CONFIG = ClusterGenerator(
	project_id=PROJECT_ID,
	zone="asia-southeast1-a",
	master_machine_type="n1-standard-4",
	master_disk_size=500,
	num_masters=1,
	num_workers=0,                          # single node mode
	idle_delete_ttl=900,                    # idle time before deleting cluster
	init_actions_uris=[f'gs://{BUCKET}/scripts/pip-install.sh'],
	metadata={'PIP_PACKAGES': 'spark-nlp'},
).make()

create_cluster_operator_task = DataprocCreateClusterOperator(
	task_id='create_dataproc_cluster',
	cluster_name="de-spark-cluster",
	project_id=PROJECT_ID,
	region="asia-southeast1",
	cluster_config=CLUSTER_GENERATOR_CONFIG
)
```
`init_actions_uris`: When the cluster is created, it will be initalised to install dependencies under `metadata` with pip

`metadata`: `spark-nlp` is required in our PySpark job

See https://github.com/GoogleCloudDataproc/initialization-actions/tree/master/python

### Submit a PySpark job to a Dataproc cluster in Airflow
Official doc: https://cloud.google.com/sdk/gcloud/reference/dataproc/jobs/submit/pyspark

The documentation lays out clearly the bash code for submitting a PySpark job.

Using the same API, my code looks like this:
```
gcloud dataproc jobs submit pyspark \
    --cluster=de-spark-cluster \
    --region=asia-southeast1 \
    --project=de-r-stocks \
    --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar \
    --properties spark.jars.packages=com.johnsnowlabs.nlp:spark-nlp_2.12:3.4.3 \
    wordcount_by_date.py \
    -- \
        --input=gs://{BUCKET}/file.parquet \
        --dataset=stocks_data
        --subreddit=stocks
        --mode=submission
```
`jars`: The JAR connector for Spark and BigQuery. This is required as the submit job will be writing processed data to a BigQuery table.
`properties`: The package for sparknlp module, which is imported during the submit job.

`wordcount_by_date.py`: The main .py file to run as the driver. It contains the PySpark code which does the preprocessing. In the above case, the file exists locally, but it can be on the cluster or in a storage bucket.

The last four arguments are custom flags passed to the driver, e.g.:

`gcloud dataproc jobs submit pyspark --cluster=my_cluster my_script.py -- --custom-flag`

These arguments will be parsed and used in the `wordcount_by_date.py` script.

Instead of using the bash code, I wrapped the above in the DataprocSubmitJobOperator. Besides the difference in API and the parameterisation, it is functionally identical.

The code looks like this (you can find it in `stocks_dag.py`):
```
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

PYSPARK_URI = f'gs://{BUCKET}/scripts/wordcount_by_date.py'


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
```
Note that the trigger_rule is not implemented correctly and can be left out.
