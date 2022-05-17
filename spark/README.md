# Documentation

## Code breakdown for `wordcount_by_date.py`
The script does the following:
- Reads parquet file from a GCS bucket
  - It assumes that the file contains the columns 'author', 'date' and 'title'
- Builds an NLP pipeline and transforms the text data in 'title' with the pipeline
  - https://nlp.johnsnowlabs.com/docs/en/install#python
  - https://github.com/JohnSnowLabs/spark-nlp-workshop/blob/master/tutorials/Certification_Trainings/Public/2.Text_Preprocessing_with_SparkNLP_Annotators_Transformers.ipynb
- Creates a dataframe that contains the token count on each date, e.g.:

| word   | wordcount | submission_date |
|--------|-------|------------|
| invest | 6     | 2022-04-11 |
| invest | 4     | 2022-04-12 |
| market | 2     | 2022-04-12 |

- Lastly, it writes dataframe containing the word count into a BigQuery table

### Write dataframe into a BigQuery table
Official doc: https://github.com/GoogleCloudDataproc/spark-bigquery-connector

```
df_wordcountbydate.write.format('bigquery') \
    .option('table', f'{dataset}.{subreddit}_{mode}_wordcount') \
    .option('temporaryGcsBucket', BUCKET) \
    .option('partitionField', f'{mode}_date') \
    .option('partitionType', 'DAY') \
    .mode('append') \
    .save()
```

This is done using a BigQuery connector for Spark. The connector must be specified when submitting the PySpark job for this script, which I did so in the Airflow DAG `stocks_dag.py`.
