from airflow.decorators import dag, task
from datetime import datetime
from io import BytesIO
import json
import pandas as pd
from minio_utils import list_objects, read_from_minio, write_to_minio
from load_to_postgres import convert_csv_to_parquet_with_schema, load_parquet_to_postgres
from airflow.models import Variable

MINIO_CONN_ID = "minio_default" 
POSTGRES_CONN_ID = "postgres_dw" 
FIRE_INCIDENTS_BUCKET = Variable.get("fire_incidents_bucket_name", default_var="fire-incidents")
STAGING_FOLDER = Variable.get("fire_incidents_staging_folder", default_var="staging")
RAW_FOLDER = Variable.get("fire_incidents_raw_folder", default_var="raw")

# Schema path - will be loaded inside tasks
data_schema_path = '/opt/airflow/dags/data_schema.json'

@dag(
    dag_id="2_full_load_minio_postgres",
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

    
    def data_quality_checks_pandas(df_original: pd.DataFrame):
        df = df_original.copy()
        dq_checks_results = {}

        analysis_columns = [ "address", "incident_date", "battalion", "neighborhood_district", "supervisor_district"]

        # Dataframe shape
        data_shape_cols = df.shape[1]
        data_shape_rows = df.shape[0]
        dq_checks_results['data_shape_cols'] = data_shape_cols
        dq_checks_results['data_shape_rows'] = data_shape_rows
        print(f'- Dataframe rows: {data_shape_rows}')
        print(f'- Dataframe columns: {data_shape_cols}')

        # columns with all values null 
        null_columns = df.columns[df.isnull().all()]
        dq_checks_results['null_columns'] = list(null_columns)
        print(f'- Null columns amount: {len(null_columns)}')

        # rows with all values null 
        null_rows = df.index[df.isnull().all(axis=1)]
        dq_checks_results['null_rows'] = str(len(null_rows))
        print(f'- Null rows amount: {len(null_rows)}')
        print(df.columns)

        # shape vs incident_number
        print(f'- Non unique incident_number: {df.shape[0] - df['incident_number'].nunique()}')
        dq_checks_results['non_unique_incident_number'] = str(df.shape[0] - df['incident_number'].nunique())

        if df.shape[0] - df['incident_number'].nunique() > 0:
            print(f'There are {df.shape[0] - df['incident_number'].nunique()} non unique incident_number. This data will be inserted on the postgres processed table, but it will be removed on curated')

        # check duplicates on analysis columns
        df_filtered = df[analysis_columns].copy()
        dup_amount = str(df_filtered.shape[0] - df_filtered.drop_duplicates().shape[0])
        dq_checks_results['dup_amount'] = dup_amount
        print(f'- Duplicate analysis duplicates amount: {dup_amount}')

        if int(dup_amount) > 0:
            print('These data will not be removed as they could be valid duplicates. These data could be multiple calls for the same incident or maybe a neighbor')
        else:
            print('No duplicate found')

        # check duplicate rows
        print(f'- Duplicate rows amount: {df.duplicated().sum()}')
        dq_checks_results['dup_rows'] = str(df.duplicated().sum())

        # check incident_number null values
        print(f'- Null incident_number amount: {df['incident_number'].isnull().sum()}')
        dq_checks_results['null_incident_number'] = str(df['incident_number'].isnull().sum())

        # Check nulls on index columns
        print(f'- Null incident_date amount: {df['incident_date'].isnull().sum()}')
        dq_checks_results['null_incident_date'] = str(df['incident_date'].isnull().sum())

        print(f'- Null battalion amount: {df['battalion'].isnull().sum()}')
        dq_checks_results['null_battalion'] = str(df['battalion'].isnull().sum())

        print(f'- Null neighborhood_district amount: {df['neighborhood_district'].isnull().sum()}')
        dq_checks_results['null_neighborhood_district'] = str(df['neighborhood_district'].isnull().sum())

        print(f'- Null supervisor_district amount: {df['supervisor_district'].isnull().sum()}')
        dq_checks_results['null_supervisor_district'] = str(df['supervisor_district'].isnull().sum())

        # List of columns with null percentage above 99%
        null_cols = list(df.columns[df.isnull().mean() > 0.99])
        dq_checks_results['null_cols'] = null_cols
        print(f'- Columns with null percentage above 99%: {null_cols}')
        print('These columns can be removed as they have too many null values')

        #save dq checks results to json
        with open('dq_checks_results.json', 'w') as f:
            json.dump(json.dumps(dq_checks_results), f)


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

            # Quality checks (only analyze as data needs to be as is in the DW)
            data_quality_checks_pandas(df)
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
