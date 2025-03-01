import time
import os
import io
from fetch_lille_bicycle import load_json_lille, extract_json_lille
from google.cloud import storage
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
import pandas as pd

current_timestamp = time.time()
current_time_struct = time.localtime(current_timestamp)
current_date_str = time.strftime("%Y%m%d", current_time_struct)
current_timestamp_for_bq = time.strftime("%Y-%m-%d %H:%M:%S", current_time_struct)
current_timestamp_str = time.strftime("%Y%m%d_%H%M%S", current_time_struct)

def load_csv_to_bucket(json_extract, blob_name, BUCKET_NAME = os.getenv('BUCKET_NAME')):
    """
    Uploads stations data dictionnary as a CSV file to the GCS bucket.
    """
    # get current time to add it in blob name
    blob_full_name = f'{blob_name}/{blob_name}_{current_date_str}/{blob_name}_{current_timestamp_str}.csv'

    df = pd.DataFrame(json_extract)
    df['GCS_loaded_at'] = current_timestamp_for_bq
    # Initialize a GCS client
    client = storage.Client()
    # Get the bucket
    bucket = client.bucket(BUCKET_NAME)
    # Create a new blob (object) in the bucket
    blob = bucket.blob(blob_full_name)
    # Convert the DataFrame to a CSV string
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    # Upload the CSV string to the blob
    blob.upload_from_string(csv_buffer.getvalue(), content_type='text/csv')
    print(f"✅ DataFrame uploaded to {blob_full_name} in bucket {BUCKET_NAME}")


def load_csv_to_bigquery(
    data_folder,
    BUCKET_NAME= os.getenv('BUCKET_NAME'),
    PROJECT_ID= os.getenv('PROJECT_ID'), 
):
    """
    function to load all the csv of the day from a bucket folder to big query
    here the GCS bucket folder take the same name of the Big Query Dataset
    """

    gcs_uri = f'gs://{BUCKET_NAME}/{data_folder}/{data_folder}_{current_date_str}/*.csv'
    
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,  # Skip header row if present
        autodetect=True,  # Automatically detect schema
    )

    # Initialize a BigQuery client
    client = bigquery.Client(project=PROJECT_ID)
    # dataset id in Big Query will have the same name of data folder in GCS
    dataset_ref = client.dataset(data_folder)
    # test if the dataset exists in BQ, create it if not
    try:
        
        client.get_dataset(dataset_ref)
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "EU"  # Vous pouvez spécifier une autre région si nécessaire
        dataset = client.create_dataset(dataset)
        print(f"dataset '{data_folder}' has been created")

    table_id = f'{data_folder}_{current_date_str}'

    # Load data into BigQuery
    load_job = client.load_table_from_uri(
        gcs_uri,
        f'{PROJECT_ID}.{data_folder}.{table_id}',
        job_config=job_config
    )

    # Wait for the job to complete
    load_job.result()

    print(f'✅ Loaded {load_job.output_rows} rows into {PROJECT_ID}:{data_folder}.{table_id}.')



if __name__ == "__main__":

    location_response = load_json_lille()
    status_dic = extract_json_lille(location_response)
    load_csv_to_bucket(status_dic, 'test_lille_status')
    load_csv_to_bigquery('test_lille_status')