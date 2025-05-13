from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from typing import Dict, List
from sql_queries import (
    drop_curated_table,
    create_curated_table, 
    query_dedup,
    query_index_battalion,
    query_index_neighborhood_district,
    query_index_supervisor_district,
    query_index_incident_date
)

POSTGRES_CONN_ID = "postgres_dw"

@dag(
    dag_id="3_create_curated_fire_incidents",
    start_date=datetime(2025, 5, 10),
    schedule="@daily",
    catchup=False,
    tags=["fire_incidents", "postgres", "curated"],
)
def create_curated_fire_incidents():
    @task(task_id="drop_curated_table")
    def drop_curated_table_task():
        """Drop the curated_fire_incidents table if it exists"""
        postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        postgres_hook.run([drop_curated_table])
        return "Table dropped successfully"

    @task(task_id="create_curated_table")
    def create_table():
        """Create the curated_fire_incidents table"""
        postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        postgres_hook.run([create_curated_table])
        return "Table created successfully"

    @task(task_id="create_curated_indexes")
    def create_indexes():
        """Create indexes on the curated_fire_incidents table"""
        postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        
        index_queries = [
            query_index_incident_date,
            query_index_battalion,
            query_index_neighborhood_district,
            query_index_supervisor_district
        ]
        
        try:
            for query in index_queries:
                if 'IF NOT EXISTS' not in query.upper():
                    query = query.replace('CREATE INDEX', 'CREATE INDEX IF NOT EXISTS')
                cur.execute(query)
                conn.commit()
                print(f"Successfully created index: {query}")
            return "Indexes created successfully"
        except Exception as e:
            conn.rollback()
            print(f"Error creating indexes: {e}")
            raise
        finally:
            cur.close()
            conn.close()

    @task(task_id="load_curated_data")
    def load_data():
        """Load data into the curated_fire_incidents table"""
        postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        
        try:
            cur.execute(query_dedup)
            conn.commit()
            print("Successfully loaded data into curated_fire_incidents table")
            return "Data loaded successfully"
        except Exception as e:
            conn.rollback()
            print(f"Error loading data: {e}")
            raise
        finally:
            cur.close()
            conn.close()
    
    # Define task dependencies
    drop_task = drop_curated_table_task()
    create_task = create_table()
    indexes_task = create_indexes()
    load_task = load_data()
    
    # Set dependencies
    drop_task >> create_task >> indexes_task >> load_task

# Create the DAG instance
create_curated_fire_incidents_dag = create_curated_fire_incidents()