
from __future__ import annotations
import time
import json
import io
from datetime import datetime
from airflow.models import Variable
from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.operators.python import PythonOperator
from google.cloud import storage
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
import pandas as pd
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
# Dag name
DAG_ID = "Station_bordeaux"
blob_name = 'bordeaux_data'
var_bucket = Variable.get('BUCKET_NAME')
var_project = Variable.get('PROJECT_ID')
var_region = Variable.get('GCP_REGION')

current_timestamp = time.time()
current_time_struct = time.localtime(current_timestamp)
current_date_str = time.strftime("%Y%m%d", current_time_struct)
current_timestamp_for_bq = time.strftime("%Y-%m-%d %H:%M:%S", current_time_struct)
current_timestamp_str = time.strftime("%Y%m%d_%H%M%S", current_time_struct)

# DAG definitions with all required params
dag = DAG(
    DAG_ID,
    default_args={"retries":1},
    tags=["bordeaux"],
    start_date=datetime(2023, 4, 26),
    catchup=False,
    #schedule_interval = '0 * * * *'
)

def transform_data(ti):
    """
    Extract needed fields from JSON response for Bordeaux stations.
    Returns a dictionary.
    """
    data = ti.xcom_pull(task_ids='extract_data_task')
    if not data:
        raise ValueError("No data found from extract_data_task")

    station_dic = {
        'lon': [],
        'lat': [],
        "insee": [],
        "commune": [],
        "gml_id": [],
        "gid": [],
        "ident": [],
        "type": [],
        "nom": [],
        "etat": [],
        "nbplaces": [],
        "nbvelos": [],
        "nbelec": [],
        "nbclassiq": [],
        "cdate": [],
        "mdate": [],
        "code_commune": []
    }

    dic_keys = list(station_dic.keys())

    print(f'🛠️ extracting Bordeaux JSON data...')
    stations = data
    for station in stations:
        for key in dic_keys:
            if key in station:
                station_dic[key].append(station.get(key))
            else:
                sub_json = station.get('geo_point_2d', {})
                station_dic[key].append(sub_json.get(key))

    print(f'✅ Bordeaux stations JSON data extracted')
    return station_dic

def load_data_to_dataframe_to_csv_to_bucket(ti):
    """
    Load extracted data into a pandas DataFrame to csv.
    """
     # get current time to add it in blob name
    blob_full_name = f'{blob_name}/{blob_name}_{current_date_str}/{blob_name}_{current_timestamp_str}.csv'
    # Initialize a GCS client
    client = storage.Client()
    # Get the bucket

    bucket = client.bucket(Variable.get('BUCKET_NAME'))
    # Create a new blob (object) in the bucket
    blob = bucket.blob(blob_full_name)
    
    
    station_dic = ti.xcom_pull(task_ids='transform_data_task')
    # df = ti.xcom_pull(task_ids='load_data_to_dataframe')
    if not station_dic:
        raise ValueError("No data found from transform_data_task task")

    df = pd.DataFrame(station_dic)
    df['GCS_loaded_at'] = current_timestamp_for_bq
    print(f'✅ Data loaded into DataFrame')
    # Convert the DataFrame to a CSV string
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    # Upload the CSV string to the blob
    blob.upload_from_string(csv_buffer.getvalue(), content_type='text/csv')
    print(f"✅ DataFrame uploaded to {blob_full_name} in bucket {Variable.get('BUCKET_NAME')}")

def load_data_gs_bigquery():
    """
    function to load all the csv of the day from a bucket folder to big query
    here the GCS bucket folder take the same name of the Big Query Dataset
    """

    gcs_uri = f'gs://{var_bucket}/{blob_name}/{blob_name}_{current_date_str}/*.csv'
    
    # Initialize a BigQuery client
    client = bigquery.Client(Variable.get('PROJECT_ID'))
    # dataset id in Big Query will have the same name of data folder in GCS
    dataset_ref = client.dataset(blob_name)
    # test if the dataset exists in BQ, create it if not
    try:      
        client.get_dataset(dataset_ref)
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "EU"  # Vous pouvez spécifier une autre région si nécessaire
        dataset = client.create_dataset(dataset)

    table_id = f'{blob_name}_{current_date_str}'
    print(f'🛠️ Loading data into BigQuery...')

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,  # Skip header row if present
        autodetect=True,  # Automatically detect schema
    )
    # Load data into BigQuery
    load_job = client.load_table_from_uri(
        source_uris = gcs_uri,
        destination = f'{var_project}.{blob_name}.{table_id}',
        job_config=job_config
    )

    # Wait for the job to complete
    load_job.result()

    print(f'✅ Loaded {load_job.output_rows} rows into {var_project}:{blob_name}.{table_id}.') 

# Task to get data from given HTTP end point
extract_data_task = HttpOperator(
    task_id="extract_data_task",
    http_conn_id="http_conn_id_velo_bordeaux",
    method="GET",
    endpoint="/api/explore/v2.1/catalog/datasets/ci_vcub_p/exports/json?lang=fr&timezone=Europe/Berlin",
    response_filter = lambda response : json.loads(response.text),
    dag=dag
)

transform_data_task = PythonOperator(
    task_id='transform_data_task',
    python_callable=transform_data
)

load_data_bucket_task = PythonOperator(
    task_id='load_data_task',
    python_callable=load_data_to_dataframe_to_csv_to_bucket,
    dag=dag
)

load_data_to_bq = PythonOperator(
    task_id='load_data_to_bq',
    python_callable=load_data_gs_bigquery,
    dag=dag
)

execute_dbt_job = CloudRunExecuteJobOperator(
    task_id="execute_dbt_job",
    project_id=var_project,
    region='europe-west9',
    job_name="dbt-transformations",
    dag=dag,
    deferrable=False,
)

# @task.external_python(python='/home/yahyaoui.kader.85/my-bicycle/soda_venv/bin/python')
# def check_load(scan_name='check_load', checks_subpath='sources'):
#     from soda.check_function import check
#     return check(scan_name, checks_subpath)

# from airflow.operators.bash import BashOperator
# BASH_COMMAND = "dbt run --profiles-dir https://storage.cloud.google.com/europe-west9-dev-composer-e-aeaa3dd5-bucket/dags/dbt/"
# operator = BashOperator(
#     task_id="dbt_run",
#     bash_command=BASH_COMMAND,
#     dag=dag
# )

# from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

# dbt_run = KubernetesPodOperator(
#     task_id='dbt_run',
#     name='dbt-run',
#     namespace='default',
#     image= 'europe-west9-docker.pkg.dev/data-engineering-451818/transformations-repository/dbt-transformations@sha256:cc0ba8b3c8dbb89ad0135bbb8bf569d56ed84293144592b2a543adae8d25568c',
#     cmds=["dbt", "run"],
#     arguments=["--profiles-dir", "/root/.dbt/profiles.yml", "--project-dir", "./dbt"],
#     # env_vars={
#     #     'GCP_REGION': GCP_REGION,
#     #     'PROJECT_ID': var_project,
#     #     'REPOSITORY_NAME': REPOSITORY_NAME,
#     #     'IMAGE_NAME': IMAGE_NAME,
#     # },
#     dag=dag
# )

# def check_load(scan_name='check_load', checks_subpath='sources'):
#     from soda.check_function import check
#     return check(scan_name, checks_subpath)

# check_load_task = PythonVirtualenvOperator(
#     task_id='check_load_task',
#     python_callable=check_load,
#     requirements=["soda-core==3.5.0, soda-core-bigquery==3.5.0"],  # Add your required packages here
#     system_site_packages=False,
#     op_kwargs={'scan_name': 'check_load', 'checks_subpath': 'sources'},
#     dag=dag
# )

# Task dependency set
extract_data_task >> transform_data_task >> load_data_bucket_task >> load_data_to_bq >> execute_dbt_job

