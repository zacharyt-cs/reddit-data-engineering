# de_r-stocks
Data engineering project on r/stocks of Reddit

## Project Overview

This project aims to review and compare subreddit comment activity on a weekly basis with the performance of the S&P500.

To accomplish this, a pipeline has to be created to ingest raw data into a data lake for storage, followed by transforming the data based on the required insights for a dashboard.

## Design

The pipeline is built with the following tools:
- Terraform - to manage the infrastructure
- Airflow - schedule data ingestion with Python scripts
- Google Cloud Storage - data lake
- BigQuery - transform raw data into standard tables
- Spark/dbt - compute subreddit activity and write data back into BigQuery
- Google Data Studio - for dashboard/visualisation

## Dashboard Tiles
1. S&P500 performance for the week
2. Number of comments for the week
3. Line chart (trend) of the number of comments for past n weeks
4. (optional) Words with highest td-idf
