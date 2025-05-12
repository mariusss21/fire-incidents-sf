from airflow.decorators import dag, task
from datetime import datetime
from io import BytesIO
import json
import pandas as pd
from minio_utils import list_objects, read_from_minio, write_to_minio
from load_to_postgres import convert_csv_to_parquet_with_schema, load_parquet_to_postgres
from airflow.models import Variable

MINIO_CONN_ID = "minio_default" # Or make this an Airflow Variable
POSTGRES_CONN_ID = "postgres_dw" # Or make this an Airflow Variable
FIRE_INCIDENTS_BUCKET = Variable.get("fire_incidents_bucket_name", default_var="fire-incidents")
STAGING_FOLDER = Variable.get("fire_incidents_staging_folder", default_var="staging")
RAW_FOLDER = Variable.get("fire_incidents_raw_folder", default_var="raw")

# Schema path - will be loaded inside tasks
data_schema_path = '/opt/airflow/dags/data_schema.json'

@dag(
    dag_id="full_load_minio_postgres",
    start_date=datetime(2025, 5, 10),
    schedule="@daily",
    catchup=False,
    tags=["fire_incidents", "minio", "etl", "processing"], 
)
def process_fire_incidents():
    @task(task_id="find_latest_file")
    def find_latest_file() -> str:
        """Find the latest CSV file in the raw folder."""
        try:
            # List all objects in raw folder
            objects = list_objects(
                bucket_name=FIRE_INCIDENTS_BUCKET,
                prefix=f"{STAGING_FOLDER}/",
                minio_conn_id=MINIO_CONN_ID
            )
            
            # Filter for CSV files and get the latest one
            csv_files = [obj for obj in objects if obj.endswith('.csv')]
            if not csv_files:
                raise ValueError(f"No CSV files found in {STAGING_FOLDER} folder")
                
            # Sort by date in filename and get the latest
            latest_file = max(csv_files, key=lambda x: x.split('_')[-1].split('.')[0])
            return latest_file
            
        except Exception as e:
            print(f"Error finding latest file: {e}")
            raise

    @task(task_id="convert_to_parquet")
    def convert_to_parquet(latest_file: str):
        """Convert the latest CSV to Parquet with proper schema."""
        try:
            # Load schema
            with open(data_schema_path, 'r') as f:
                data_schema = json.load(f)

            # Read CSV from MinIO
            csv_bytes = read_from_minio(
                bucket_name=FIRE_INCIDENTS_BUCKET,
                object_name=latest_file,
                minio_conn_id=MINIO_CONN_ID
            )
            
            # Convert bytes to pandas DataFrame
            df = pd.read_csv(BytesIO(csv_bytes))
            
            # Create parquet file name
            parquet_name = f"{RAW_FOLDER}/{latest_file.split('/')[1].split('.')[0]}.parquet"
            
            # Convert to parquet with schema
            convert_csv_to_parquet_with_schema(
                df=df,
                schema=data_schema,
                minio_bucket_name=FIRE_INCIDENTS_BUCKET,
                minio_object_name=parquet_name,
                minio_conn_id=MINIO_CONN_ID
            )
            
            return parquet_name
            
        except Exception as e:
            print(f"Error converting to parquet: {e}")
            raise

    @task(task_id="load_to_postgres")
    def load_to_postgres(parquet_file: str):
        """Load the parquet file into PostgreSQL."""
        try:
            load_parquet_to_postgres(
                bucket_name=FIRE_INCIDENTS_BUCKET,
                object_name=parquet_file,
                minio_conn_id=MINIO_CONN_ID,
                postgres_conn_id=POSTGRES_CONN_ID
            )
            
        except Exception as e:
            print(f"Error loading to PostgreSQL: {e}")
            raise

    # Define task dependencies
    latest_file = find_latest_file()
    parquet_file = convert_to_parquet(latest_file)
    load_to_postgres(parquet_file)

# Create the DAG
process_fire_incidents_dag = process_fire_incidents()
