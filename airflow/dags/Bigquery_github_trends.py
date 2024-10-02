#import json
from datetime import timedelta, datetime

from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import (
  
    BigQueryInsertJobOperator,
    BigQueryCheckOperator
)


# Config variables
dag_config = Variable.get("bigquery_github_trends_variables", deserialize_json=True)
BQ_CONN_ID = dag_config["bq_conn_id"]
BQ_PROJECT = dag_config["bq_project"]
BQ_DATASET = dag_config["bq_dataset"]

# Define default arguments for the DAG
default_args = {
    'owner': 'sadkianas',
    'depends_on_past': True,
    'start_date': datetime(2024, 9, 1),  
    'email': ['anassadki73@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id="bigquery_github_trends",
    schedule_interval="@daily",
    default_args=default_args,  
    catchup=False,
    tags=["example", "bigquery"],
) as dag:

     # Task to check if the GitHub archive data has a dated table for that date
    t1 = BigQueryCheckOperator(
        task_id='bq_check_githubarchive_day',
        sql='''
        #standardSQL
        SELECT
          table_id
        FROM
          `githubarchive.day.__TABLES_SUMMARY__`
        WHERE
          table_id = "{{ yesterday_ds_nodash }}"
        ''',
        use_legacy_sql=False,
       dag=dag
    )
    # Task 2: Check that the Hacker News table contains data for that date
    t2 = BigQueryCheckOperator(
        task_id='bq_check_hackernews_full',
        sql=''' 
        #standardSQL
        SELECT
        FORMAT_TIMESTAMP("%Y%m%d", timestamp) AS date
        FROM
        `bigquery-public-data.hacker_news.full`
        WHERE
        type = 'story'
        AND FORMAT_TIMESTAMP("%Y%m%d", timestamp) = "{{ yesterday_ds_nodash }}"
        LIMIT
        1
        ''',
        use_legacy_sql=False,
        dag=dag
    )
    t3 = BigQueryInsertJobOperator(
        task_id='bq_write_to_github_daily_metrics',
        configuration={
            "query": {
                "query": '''
                    #standardSQL
                       SELECT
                        DATE(created_at) AS date,
                        repo,
                        SUM(IF(type = 'WatchEvent', 1, NULL)) AS stars,
                        SUM(IF(type = 'ForkEvent', 1, NULL)) AS forks
                        FROM (
                        SELECT
                            created_at,
                            actor.id AS actor_id,
                            repo.name AS repo,
                            type
                        FROM
                            `githubarchive.day.{{ yesterday_ds_nodash }}`
                        WHERE
                            type IN ('WatchEvent', 'ForkEvent')
                        )
                        GROUP BY
                        date,
                        repo
                                            ''',
                "destinationTable": {
                    "projectId": BQ_PROJECT,
                    "datasetId": BQ_DATASET,
                    "tableId": "github_daily_metrics",
                    "timePartitioning": {
                    "type": "DAY",
                    "field": "date"  
                }
                },
                "writeDisposition": "WRITE_TRUNCATE",
                    "useLegacySql": False  
                    }
        },
        
        gcp_conn_id=BQ_CONN_ID,
        dag=dag
    )


    # Task 4: Aggregate past GitHub events to daily table
    t4 = BigQueryInsertJobOperator(
        task_id='bq_write_to_github_agg',
        configuration={
            "query": {
                "query": '''
                    #standardSQL
                    SELECT
                    "{2}" AS date,
                    repo,
                    SUM(stars) AS stars_last_28_days,
                    SUM(IF(date BETWEEN DATE("{4}") 
                        AND DATE("{3}"), stars, NULL)) AS stars_last_7_days,
                    SUM(IF(date = DATE("{3}"), stars, NULL)) AS stars_last_1_day,
                    SUM(forks) AS forks_last_28_days,
                    SUM(IF(date BETWEEN DATE("{4}") 
                        AND DATE("{3}"), forks, NULL)) AS forks_last_7_days,
                    SUM(IF(date = DATE("{3}"), forks, NULL)) AS forks_last_1_day
                    FROM
                    `{0}.{1}.github_daily_metrics`
                    WHERE date BETWEEN DATE("{5}") 
                    AND DATE("{3}") 
                    GROUP BY
                    date,
                    repo
                '''.format(
                    BQ_PROJECT, BQ_DATASET,
                    "{{ yesterday_ds_nodash }}",  
                    "{{ yesterday_ds }}",          
                    "{{ macros.ds_add(ds, -6) }}",  
                    "{{ macros.ds_add(ds, -27) }}"  
                ),
                "destinationTable": {
                    "projectId": BQ_PROJECT,
                    "datasetId": BQ_DATASET,
                    "tableId": "github_agg_metrics"  
                },
                "writeDisposition": "WRITE_TRUNCATE",  
                "useLegacySql": False  
            }
        },
        gcp_conn_id=BQ_CONN_ID,
        dag=dag,
    )

  # Task 5: Aggregate hacker news data to a daily partition table
    t5 = BigQueryInsertJobOperator(
        task_id='bq_write_to_hackernews_agg',
        configuration={
            "query": {
                "query": '''
                    #standardSQL
                    SELECT
                    DATE(timestamp) AS date,
                    `by` AS submitter,
                    id as story_id,
                    REGEXP_EXTRACT(url, "(https?://github.com/[^/]*/[^/#?]*)") as url,
                    SUM(score) as score
                    FROM
                    `bigquery-public-data.hacker_news.full`
                    WHERE
                    type = 'story'
                    AND timestamp > '{{ yesterday_ds }}'
                    AND timestamp < '{{ ds }}'
                    AND url LIKE '%https://github.com%'
                    AND url NOT LIKE '%github.com/blog/%'
                    GROUP BY
                    date,
                    submitter,
                    story_id,
                    url
                ''',
                "destinationTable": {
                    "projectId": BQ_PROJECT,
                    "datasetId": BQ_DATASET,
                    "tableId": "hackernews_agg",  
                },
                "writeDisposition": "WRITE_TRUNCATE",
                "useLegacySql": False,
                
            }
        },
        gcp_conn_id=BQ_CONN_ID,
        dag=dag
    )

    # Task 6: Join the aggregate tables
    t6 = BigQueryInsertJobOperator(
        task_id='bq_write_to_hackernews_github_agg',    
        configuration={
            "query": {
                "query": '''
                    #standardSQL
                    SELECT 
                        a.date AS date,
                        a.url AS github_url,
                        b.repo AS github_repo,
                        a.score AS hn_score,
                        a.story_id AS hn_story_id,
                        b.stars_last_28_days AS stars_last_28_days,
                        b.stars_last_7_days AS stars_last_7_days,
                        b.stars_last_1_day AS stars_last_1_day,
                        b.forks_last_28_days AS forks_last_28_days,
                        b.forks_last_7_days AS forks_last_7_days,
                        b.forks_last_1_day AS forks_last_1_day
                    FROM
                        `{0}.{1}.hackernews_agg` AS a
                    LEFT JOIN 
                        `{0}.{1}.github_agg_metrics` AS b
                    ON 
                        a.url = b.repo
                    WHERE 
                        a.date = DATE("{2}")
                '''.format(
                    BQ_PROJECT, 
                    BQ_DATASET, 
                    "{{ yesterday_ds }}"  
                ),
                "destinationTable": {
                    "projectId": BQ_PROJECT,
                    "datasetId": BQ_DATASET,
                    "tableId": "hackernews_github_agg"  
                },
                "writeDisposition": "WRITE_TRUNCATE",  
                "useLegacySql": False  
            }
        },
        gcp_conn_id=BQ_CONN_ID,
        dag=dag,
    )

    # Task 7: Check if data is written successfully
    t7 = BigQueryCheckOperator(
        task_id='bq_check_hackernews_github_agg',
        sql=''' 
        #standardSQL
        SELECT
            COUNT(*) AS rows_in_table
        FROM `{0}.{1}.hackernews_github_agg`
        WHERE date = DATE("{2}")
        '''.format(BQ_PROJECT, BQ_DATASET, '{{ yesterday_ds }}'),
        use_legacy_sql=False,
        dag=dag
    )


    # Set task dependencies 
t1 >> t3 >> t4
t2 >> t5
t4 >> t6
t5 >> t6
t6 >> t7
