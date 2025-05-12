import pandas as pd
import io
from minio_utils import read_from_minio, write_to_minio
import os
from io import BytesIO
from airflow.providers.postgres.hooks.postgres import PostgresHook

def convert_csv_to_parquet_with_schema(
    df: pd.DataFrame,
    schema: dict,
    minio_bucket_name: str,
    minio_object_name: str,
    minio_conn_id: str = "minio_default"
):
    """
    Reads a CSV file, applies specified data types to columns based on the schema,
    and uploads the resulting DataFrame as a Parquet object to MinIO.

    Args:
        csv_file_path (str): Path to the input CSV file.
        schema (dict): A dictionary where keys are column names and values are
                       target data types ('Text', 'Number', 'Floating Timestamp', 'Point').
        minio_bucket_name (str): Name of the MinIO bucket.
        minio_object_name (str): Name of the object (file path) in MinIO for the Parquet file.
        minio_conn_id (str): Airflow connection ID for MinIO.
    """
    print("Starting CSV to Parquet conversion")

    original_columns = df.columns.tolist()
    df.columns = [col.lower().replace(' ', '_').replace('-', '_') for col in df.columns]

    processed_columns = set()

    for col_name, target_type in schema.items():
        if col_name not in df.columns:
            print(f"Warning: Column '{col_name}' defined in schema not found in CSV. Original CSV columns: {original_columns}")
            continue

        processed_columns.add(col_name)
        print(f"Processing column: '{col_name}' as {target_type}")

        try:
            if target_type == 'Floating Timestamp':
                df[col_name] = pd.to_datetime(df[col_name], errors='coerce')
            elif target_type == 'Number':
                df[col_name] = pd.to_numeric(df[col_name], errors='coerce')
                if df[col_name].dropna().mod(1).eq(0).all():
                    df[col_name] = df[col_name].astype('Int64')
                else:
                    df[col_name] = df[col_name].astype('float64')
            elif target_type == 'Text':
                df[col_name] = df[col_name].astype(str)
            elif target_type == 'Point':
                df[col_name] = df[col_name].astype(str)
            else:
                print(f"Warning: Unknown target type '{target_type}' for column '{col_name}'. Skipping type conversion.")
        except Exception as e:
            print(f"Error converting column '{col_name}' to {target_type}: {e}. Column will be left as is or as partially converted.")

    extra_columns = set(df.columns) - processed_columns

    df = df.rename(
        columns={
             "automatic_extinguishing_sytem_type": "automatic_extinguishing_system_type",
             "automatic_extinguishing_sytem_perfomance": "automatic_extinguishing_system_performance",
             "automatic_extinguishing_sytem_failure_reason": "automatic_extinguishing_system_failure_reason",   
        }
    )
    if extra_columns:
        print(f"Info: Columns present in CSV but not in schema (will retain inferred types): {list(extra_columns)}")

    try:
        print(f"Writing DataFrame to in-memory Parquet buffer...")
        parquet_buffer = BytesIO()
        df.to_parquet(parquet_buffer, engine='pyarrow', index=False)
        parquet_buffer.seek(0)

        write_to_minio(
            buffer=parquet_buffer,
            bucket_name = minio_bucket_name,
            object_name = minio_object_name,
            minio_conn_id=minio_conn_id
        )

        print(f"Successfully converted and uploaded to MinIO: {minio_bucket_name}/{minio_object_name}")
    except Exception as e:
        print(f"Error writing Parquet to MinIO ({minio_bucket_name}/{minio_object_name}): {e}")


def create_database_and_table(postgres_conn_id: str = "postgres_dw"):
    """
    Create PostgreSQL database and table for fire incidents data
    """
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    conn = hook.get_conn()
    conn.autocommit = True
    cursor = conn.cursor()
    
    try:
        # Drop table if it exists
        cursor.execute("DROP TABLE IF EXISTS fire_incidents")
        print("Existing table dropped if it existed")
        
        print("Attempting to enable PostGIS extension...")
        cursor.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
        print("PostGIS extension enabled or already exists.")

        # Create table
        cursor.execute("""
            CREATE TABLE fire_incidents (
                incident_number VARCHAR(50) PRIMARY KEY,
                exposure_number INTEGER,
                id VARCHAR(50),
                address VARCHAR(200),
                incident_date TIMESTAMP,
                call_number VARCHAR(50),
                alarm_dttm TIMESTAMP,
                arrival_dttm TIMESTAMP,
                close_dttm TIMESTAMP,
                city VARCHAR(100),
                zipcode VARCHAR(20),
                battalion VARCHAR(50),
                station_area VARCHAR(50),
                box VARCHAR(20),
                suppression_units INTEGER,
                suppression_personnel INTEGER,
                ems_units INTEGER,
                ems_personnel INTEGER,
                other_units INTEGER,
                other_personnel INTEGER,
                first_unit_on_scene VARCHAR(100),
                estimated_property_loss INTEGER,
                estimated_contents_loss INTEGER,
                fire_fatalities INTEGER,
                fire_injuries INTEGER,
                civilian_fatalities INTEGER,
                civilian_injuries INTEGER,
                number_of_alarms INTEGER,
                primary_situation VARCHAR(100),
                mutual_aid VARCHAR(50),
                action_taken_primary VARCHAR(100),
                action_taken_secondary VARCHAR(100),
                action_taken_other VARCHAR(100),
                detector_alerted_occupants VARCHAR(50),
                property_use VARCHAR(100),
                area_of_fire_origin VARCHAR(100),
                ignition_cause VARCHAR(100),
                ignition_factor_primary VARCHAR(100),
                ignition_factor_secondary VARCHAR(100),
                heat_source VARCHAR(100),
                item_first_ignited VARCHAR(100),
                human_factors_associated_with_ignition VARCHAR(100),
                structure_type VARCHAR(100),
                structure_status VARCHAR(100),
                floor_of_fire_origin INTEGER,
                fire_spread VARCHAR(100),
                no_flame_spread VARCHAR(50),
                number_of_floors_with_minimum_damage INTEGER,
                number_of_floors_with_significant_damage INTEGER,
                number_of_floors_with_heavy_damage INTEGER,
                number_of_floors_with_extreme_damage INTEGER,
                detectors_present VARCHAR(50),
                detector_type VARCHAR(100),
                detector_operation VARCHAR(100),
                detector_effectiveness VARCHAR(100),
                detector_failure_reason VARCHAR(100),
                automatic_extinguishing_system_present VARCHAR(50),
                automatic_extinguishing_system_type VARCHAR(100),
                automatic_extinguishing_system_performance VARCHAR(100),
                automatic_extinguishing_system_failure_reason VARCHAR(100),
                number_of_sprinkler_heads_operating INTEGER,
                supervisor_district VARCHAR(50),
                neighborhood_district VARCHAR(100),
                point GEOMETRY(POINT, 4326),
                data_as_of TIMESTAMP,
                data_loaded_at TIMESTAMP
            )
        """)
        print("Table created successfully")
        
    except psycopg2.Error as e:
        print(f"Error creating table: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

def load_parquet_to_postgres(
    bucket_name: str,
    object_name: str,
    minio_conn_id: str = "minio_default",
    postgres_conn_id: str = "postgres_dw"
):
    """
    Read parquet file from MinIO and load it into PostgreSQL
    """
    try:
        # Read parquet from MinIO
        parquet_bytes = read_from_minio(bucket_name, object_name, minio_conn_id=minio_conn_id)

        # Convert bytes to pandas DataFrame
        df = pd.read_parquet(BytesIO(parquet_bytes))  
        df = df.drop_duplicates()                                                  

        # Create database and table
        create_database_and_table(postgres_conn_id=postgres_conn_id)
        
        # Create SQLAlchemy engine
        hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        engine = hook.get_sqlalchemy_engine()
        
        # Load data into PostgreSQL
        df.to_sql('fire_incidents', engine, if_exists='replace', index=False)
        print(f"Successfully loaded {len(df)} records into PostgreSQL")
        
    except Exception as e:
        print(f"Error loading data: {e}")
        raise
