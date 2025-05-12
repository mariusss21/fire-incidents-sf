import pandas as pd
from typing import Optional
import io
from datetime import datetime
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def write_to_minio(
    bucket_name: str,
    file_path: str = None,
    buffer: io.BytesIO = None,
    object_name: Optional[str] = None,
    minio_conn_id: str = "minio_default"
) -> None:
    """
    Write data to MinIO storage. Supports both file paths and buffer inputs.
    
    Args:
        bucket_name: Name of the MinIO bucket
        object_name: Name of the object in MinIO (if None, uses file name)
        endpoint_url: MinIO endpoint URL
        access_key: MinIO access key
        secret_key: MinIO secret key
    
    Raises:
        ValueError: If neither file_path nor buffer is provided
    """
    if not file_path and not buffer:
        raise ValueError("Either file_path or buffer must be provided")
    
    hook = S3Hook(aws_conn_id=minio_conn_id)
    client = hook.get_conn() # This is a boto3.client('s3') object
    
    # If object_name is not provided, use the file name
    if object_name is None:
        if file_path:
            object_name = file_path.split('/')[-1]
        else:
            # Generate a timestamp-based name for buffer
            object_name = f"buffer_upload_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    try:
        if file_path:
            # Upload from file path
            client.upload_file(file_path, bucket_name, object_name)
        else:
            # Upload from buffer
            client.upload_fileobj(buffer, bucket_name, object_name)
        
        print(f"Successfully uploaded to {bucket_name}/{object_name}")
    except Exception as e:
        print(f"Error uploading data: {str(e)}")
        raise

def list_objects(
    bucket_name: str,
    prefix: str = "",
    minio_conn_id: str = "minio_default"
) -> list:
    """
    List objects in a MinIO bucket.
    
    Args:
        bucket_name: Name of the MinIO bucket
        prefix: Prefix to filter objects (optional)
        minio_conn_id: Airflow connection ID for MinIO
    
    Returns:
        list: List of object names
    """
    hook = S3Hook(aws_conn_id=minio_conn_id)
    client = hook.get_conn()
    
    try:
        response = client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        if 'Contents' in response:
            return [obj['Key'] for obj in response['Contents']]
        return []
    except Exception as e:
        print(f"Error listing objects: {str(e)}")
        raise


def read_from_minio(
    bucket_name: str,
    object_name: str,
    minio_conn_id: str = "minio_default"
) -> bytes:
    """
    Read a file from MinIO storage.
    
    Args:
        bucket_name: Name of the MinIO bucket
        object_name: Name of the object in MinIO
        minio_conn_id: Airflow connection ID for MinIO
    
    Returns:
        bytes: The file content as bytes
    """
    hook = S3Hook(aws_conn_id=minio_conn_id)
    client = hook.get_conn()
    
    try:
        response = client.get_object(Bucket=bucket_name, Key=object_name)
        return response['Body'].read()
    except Exception as e:
        print(f"Error reading file: {str(e)}")
        raise