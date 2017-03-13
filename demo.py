from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator
from airflow.contrib.operators.bigquery_to_bigquery import BigQueryToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from datetime import datetime, date, timedelta

# Apply default options for all tasks in the pipeline. These can be overridden in each task

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': datetime(2017, 02, 22),
    #'end_date': datetime(2016, 11, 5),
    'email': ['richradley@google.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

# Define the pipeline schedule
schedule_interval = '@daily'

# Define the daq
dag = DAG('demo', default_args=default_args, schedule_interval=schedule_interval)

# define current days date
current_date = '{{ macros.ds_format(ds, "%Y-%m-%d", "%Y%m%d") }}'

# Task 1: Upload Referendum Data to BigQuery EU
# airflow test -sd=airflow_dags demo gcs_to_bq_eu 2017-03-06

schema_fields = [
    {'name': 'id', 'type': 'STRING'},
    {'name': 'region_code', 'type': 'STRING'},
    {'name': 'region', 'type': 'STRING'},
    {'name': 'area_code', 'type': 'STRING'},
    {'name': 'area', 'type': 'STRING'},
    {'name': 'electorate', 'type': 'STRING'},
    {'name': 'expected_ballots', 'type': 'STRING'},
    {'name': 'verified_ballot_papers', 'type': 'STRING'},
    {'name': 'pct_turnout', 'type': 'STRING'},
    {'name': 'votes_cast', 'type': 'STRING'},
    {'name': 'valid_votes', 'type': 'STRING'},
    {'name': 'remain', 'type': 'STRING'},
    {'name': 'leave', 'type': 'STRING'},
    {'name': 'rejected', 'type': 'STRING'},
    {'name': 'no_official_mark', 'type': 'STRING'},
    {'name': 'voting_for_both', 'type': 'STRING'},
    {'name': 'writing_or_mark', 'type': 'STRING'},
    {'name': 'void', 'type': 'STRING'},
    {'name': 'pct_remain', 'type': 'STRING'},
    {'name': 'pct_leave', 'type': 'STRING'},
    {'name': 'pct_rejected', 'type': 'STRING'}
    ]

t1 = GoogleCloudStorageToBigQueryOperator(
    task_id='gcs_to_bq_eu',
    bucket='rradley-bq-data',
    schema_fields=schema_fields,
    skip_leading_rows=1,
    source_objects=['eu-referendum-result-data.csv'],
    destination_project_dataset_table='airflow_referendum.result_data',
    dag=dag)


# Task 2: Check admin areas table exists
# airflow test -sd=airflow_dags demo bq_dest_table_lookup_admin 2017-03-06

t2 = BigQueryCheckOperator(
    task_id='bq_dest_table_lookup_admin',
    sql='SELECT table_id FROM [google.com:rich-lab:airflow_referendum.__TABLES__] WHERE table_id = "admin_areas"',
    dag=dag)


# Task 3: Join DCM table with lookup files
# airflow test -sd=airflow_dags demo bq_dest_populate 2017-03-06

t3 = BigQueryOperator(
    task_id='bq_dest_populate',
    bql='demo/bigquery_sql/geo.sql',
    destination_dataset_table='airflow_referendum.geo_results{}'.format(current_date),
    write_disposition='WRITE_TRUNCATE',
    dag=dag)

t2.set_upstream(t1)
t3.set_upstream(t2)
