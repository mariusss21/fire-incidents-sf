from airflow.decorators import dag, task
from datetime import datetime
from extract_fire_incidents_data import extract_fire_incidents_parameters, download_fire_incidents_data, delete_local_file
from minio_utils import write_to_minio
import os
from airflow.models import Variable

@dag(
    dag_id="1_fire_incidents_etl_minio",
    start_date=datetime(2025, 5, 10),
    schedule="@daily",
    catchup=False,
    tags=["fire_incidents", "minio", "etl", "scraping"], 
)
def fire_incidents_etl():
    
    FIRE_INCIDENTS_BUCKET = Variable.get("fire_incidents_bucket_name", default_var="fire-incidents")
    STAGING_FOLDER = Variable.get("fire_incidents_staging_folder", default_var="staging")

    @task(task_id="extract_parameters")
    def extract_params():
        """Extract fire incidents parameters from the source."""
        cache_bust, date = extract_fire_incidents_parameters()
        return {'cache_bust': cache_bust, 'date': date}

    @task(task_id="download_data")
    def download_data(fire_params: dict):
        """Download fire incidents data using the extracted parameters."""
        return download_fire_incidents_data(fire_params['cache_bust'], fire_params['date'])

    @task(task_id="upload_to_minio")
    def upload_to_minio(filename: str):
        """Upload the downloaded file to MinIO."""
        # Upload to MinIO staging folder
        write_to_minio(
            bucket_name=FIRE_INCIDENTS_BUCKET,
            file_path=filename,
            object_name=f"{STAGING_FOLDER}/{filename}",
        )
        return filename

    @task(task_id="cleanup")
    def cleanup(filename: str):
        """Delete the local file after successful upload."""
        delete_local_file(filename)

    # Define task dependencies
    fire_params = extract_params()
    filename = download_data(fire_params)
    uploaded_file = upload_to_minio(filename)
    cleanup(uploaded_file)

# Create the DAG
fire_incidents_etl_dag = fire_incidents_etl()
