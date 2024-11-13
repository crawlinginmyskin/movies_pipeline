import datetime
import json

from airflow import models
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from airflow.operators import empty, python


PROJECT = "project"
REGION = "region"
JOB_ID = 'omdb-downloader'
SOURCE_BUCKET = "some-bucket"
SCHEMA_DIR = "/home/airflow/gcs/dags/schema"
DBT_DATASET = 'prod'
DBT_REGION = 'some-region'
DBT_JOB_NAME = "dbt"

with models.DAG(
    'movies_pipeline_v1',
    schedule_interval="45 9 * * *",
    default_args={"owner": "FZ"},
    catchup=False,
    start_date=datetime.datetime(2024,11,8)
) as dag:
    
    execute = CloudRunExecuteJobOperator(
        task_id = 'run_downloader',
        project_id=PROJECT,
        region=REGION,
        job_name=JOB_ID,
        overrides={
            "container_overrides": [
                {
                    "env": [{"name": "BUCKET", "value": SOURCE_BUCKET}, {"name": "PROJECT_ID", "value": PROJECT}]
                }
            ]
        }
    )
    list_files = GCSListObjectsOperator(
        task_id="list_files",
        bucket=SOURCE_BUCKET,
        prefix="{{data_interval_end.strftime('%Y-%m-%d')}}/"
    )
    def check_gcs_list_objects(**kwargs):
        ti = kwargs['ti']
        file_list = ti.xcom_pull(task_ids="list_files")
        if not file_list:
            return 'no_new_data'
        else:
            return 'load_movies'

    new_data_check = python.BranchPythonOperator(task_id="check_for_new_data", provide_context=True, python_callable=check_gcs_list_objects)
    no_new_data = empty.EmptyOperator(task_id="no_new_data")
    load_movies = empty.EmptyOperator(task_id="load_movies")

    with open(f"{SCHEMA_DIR}/movie_fact.json", "r") as f:
        movie_fact_schema = json.load(f)

    movie_fact_load = GCSToBigQueryOperator(
        task_id = "load_movie_fact",
        bucket = SOURCE_BUCKET,
        source_objects=["{{data_interval_end.strftime('%Y-%m-%d')}}/movie_fact.csv"],
        project_id=PROJECT,
        create_disposition="CREATE_NEVER",
        destination_project_dataset_table=f"{PROJECT}.source.stg_movie_fact",
        schema_fields = movie_fact_schema,
        write_disposition="WRITE_TRUNCATE",
        dag=dag,
        deferrable=False
    )

    with open(f"{SCHEMA_DIR}/movie_info_dim.json", "r") as f:
        movie_info_dim_schema = json.load(f)

    movie_info_dim_load = GCSToBigQueryOperator(
        task_id = "load_movie_info_dim",
        bucket = SOURCE_BUCKET,
        source_objects=["{{data_interval_end.strftime('%Y-%m-%d')}}/movie_info_dim.csv"],
        project_id=PROJECT,
        create_disposition="CREATE_NEVER",
        destination_project_dataset_table=f"{PROJECT}.source.stg_movie_info_dim",
        schema_fields = movie_info_dim_schema,
        write_disposition="WRITE_TRUNCATE",
        dag=dag,
        deferrable=False)

    with open(f"{SCHEMA_DIR}/ratings_dim.json", "r") as f:
        ratings_dim_schema = json.load(f)

    ratings_dim_load = GCSToBigQueryOperator(
        task_id = "load_ratings_dim",
        bucket = SOURCE_BUCKET,
        source_objects=["{{data_interval_end.strftime('%Y-%m-%d')}}/ratings_dim.csv"],
        project_id=PROJECT,
        create_disposition="CREATE_NEVER",
        destination_project_dataset_table=f"{PROJECT}.source.stg_ratings_dim",
        schema_fields = ratings_dim_schema,
        write_disposition="WRITE_TRUNCATE",
        dag=dag,
        deferrable=False
    )

    dbt_run = CloudRunExecuteJobOperator(
        task_id = 'dbt_run',
        project_id=PROJECT,
        region=DBT_REGION,
        job_name=DBT_JOB_NAME,
        overrides={
            "container_overrides": [
                {
                    "env": [{"name": "DBT_DATASET", "value": DBT_DATASET}, 
                            {"name": "DBT_PROJECT", "value": PROJECT},
                            {"name": "DBT_LOCATION", "value": DBT_REGION},]
                }
            ]
        }
    ) 

    execute >> list_files >>new_data_check
    new_data_check >> [no_new_data, load_movies]
    load_movies >> movie_info_dim_load
    load_movies >> movie_fact_load
    load_movies >> ratings_dim_load
    movie_info_dim_load >> dbt_run
    movie_fact_load >> dbt_run
    ratings_dim_load >> dbt_run
