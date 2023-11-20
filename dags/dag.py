from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from google.cloud import bigquery
import pandas as pd
import logging
import os
from sqlalchemy import create_engine

# Constraints
CSV_FILES = ['/home/kat/Documents/elama/csv_files/webinar.csv', '/home/kat/Documents/elama/csv_files/users.csv', '/home/kat/Documents/elama/csv_files/transactions.csv']
TABLE_NAMES = ['webinar', 'users', 'transactions']
POSTGRES_CONN_ID = 'postgres_default'
PROJECT_ID = 'unified-skein-405415'
TABLE_ID = f"{PROJECT_ID}.elama.saw"
MATERIALIZED_VIEW_NAME = 'saw'
os.environ['GOOGLE_CLOUD_PROJECT'] = PROJECT_ID

# Create PostgreSQL Engine
POSTGRES_ENGINE = create_engine(f'postgresql+psycopg2://postgres:postgres@127.0.0.1:5432/elama')

def load_csv_to_postgres(csv_files, table_names, postgres_engine):
    hook = PostgresHook(POSTGRES_CONN_ID)
    conn = hook.get_conn()
    logging.info(f"Connected to PostgreSQL: {conn}")

    for csv_file, table_name in zip(csv_files, table_names):
        df = pd.read_csv(csv_file)
        df.to_sql(table_name, postgres_engine, index=False, if_exists='replace')

def create_materialized_view(postgres_engine):
    query = """
    CREATE MATERIALIZED VIEW IF NOT EXISTS saw AS
    SELECT u.email, MIN(u.user_id) AS user_id, MIN(CAST(u.date_registration AS DATE)) AS registration_date, SUM(t.price) AS total_transactions
    FROM users u
    JOIN transactions t ON u.user_id = t.user_id
    WHERE u.email IN (SELECT email FROM webinar) AND CAST(u.date_registration AS DATE) > '2016-04-01'::date
    GROUP BY u.email;
    """
    with postgres_engine.connect() as connection:
        connection.execute(query)

def write_df_to_bigquery(postgres_engine):
    df = pd.read_sql(f'SELECT * FROM {MATERIALIZED_VIEW_NAME}', postgres_engine)
    client = bigquery.Client(project=PROJECT_ID)
    job_config = bigquery.LoadJobConfig(write_disposition='WRITE_TRUNCATE', autodetect=True)
    job = client.load_table_from_dataframe(df, TABLE_ID, job_config=job_config)
    job.result()

# DAG Configuration
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('elama', default_args=default_args, schedule_interval=None)

# DAG Task
task_load_and_create = PythonOperator(
    task_id='load_and_create_tables',
    python_callable=load_csv_to_postgres,
    op_args=[CSV_FILES, TABLE_NAMES, POSTGRES_ENGINE],
    dag=dag,
)

task_create_materialized_view = PythonOperator(
    task_id='create_view',
    python_callable=create_materialized_view,
    op_args=[POSTGRES_ENGINE],
    dag=dag,
)

task_write_to_bigquery = PythonOperator(
    task_id='write_to_bigquery',
    python_callable=write_df_to_bigquery,
    op_args=[POSTGRES_ENGINE],
    dag=dag,
)

task_load_and_create >> task_create_materialized_view >> task_write_to_bigquery