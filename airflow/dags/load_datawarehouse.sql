-- create temp table (with modification)
CREATE OR REPLACE TEMP TABLE {{ subreddit }}_{{ mode }}
AS
SELECT id, title, author, num_comments, total_awards_received, DATE(TIMESTAMP_SECONDS(created_utc)) AS {{ mode }}_date
FROM {{ BIGQUERY_DATASET }}.{{ subreddit }}_{{ mode }}_external_table;

-- if permanent table does not exist, create permanent table from temp table (with partition)
CREATE TABLE IF NOT EXISTS {{ BIGQUERY_DATASET }}.{{ subreddit }}_{{ mode }}_all
PARTITION BY {{ mode }}_date
AS 
SELECT * FROM {{ subreddit }}_{{ mode }};

-- maintain idempotency using delete-write
-- delete rows from permanent table (only delete data that the pipeline will re-create)
DELETE {{ BIGQUERY_DATASET }}.{{ subreddit }}_{{ mode }}_all
WHERE {{ mode }}_date BETWEEN '{{ ds }}' AND '{{ macros.ds_add(data_interval_end.strftime('%Y-%m-%d'), -1) }}';

-- insert data from temp table to permanent table
INSERT INTO {{ BIGQUERY_DATASET }}.{{ subreddit }}_{{ mode }}_all
SELECT * FROM {{ subreddit }}_{{ mode }};