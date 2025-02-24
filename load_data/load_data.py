# Project 7 - Data Pipeline with Python, Docker, DBT, BigQuery, and Looker Studio

# Imports
import os
import sqlalchemy
import pandas as pd
from google.cloud import bigquery
from config import DATABASE_URL, PROJECT_ID, DATASET_ID, GCS_CREDENTIALS_PATH

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GCS_CREDENTIALS_PATH

client = bigquery.Client(project=PROJECT_ID)

def create_dataset(dataset_id):
    """Creates a dataset in BigQuery if it doesn't exist."""
    dataset_ref = f"{PROJECT_ID}.{dataset_id}"
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = "US"

    try:
        client.get_dataset(dataset_ref) 
    except Exception:
        client.create_dataset(dataset)

def load_bigquery(table_name, df):
    """Loads a Pandas DataFrame into BigQuery."""
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True  # Automatically detect schema
    )

    try:
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()  # Wait for completion
        print(f"{table_name} created")
    except Exception as e:
        print(f"Error loading data into BigQuery: {e}")

def extract_and_load_devices(table_name,query):
    """Extracts data from PostgreSQL and loads it into BigQuery."""
    try:
        # Create dataset if it doesn't exist
        create_dataset(DATASET_ID)

        # Connect to PostgreSQL
        engine = sqlalchemy.create_engine(DATABASE_URL)
        df = pd.read_sql(query, con=engine)

        if df.empty:
            print(f"No data found in the {table_name} table.")
            return

        load_bigquery(table_name, df)
 
    except Exception as e:
        print(f"Error in ETL process: {e}")

tables = [
    {
        "table_name": "stg_devices",
        "query": "SELECT id, name FROM bg3.devices;"
    },
    {
        "table_name": "stg_countries",
        "query": "SELECT id, name, continent FROM bg3.countries;"
    },
    {
        "table_name": "stg_sessions",
        "query": "SELECT id, start_date, end_date, device_id, user_id, country_access_id FROM bg3.session;"
    },
    {
        "table_name": "stg_users",
        "query": "SELECT id, country_id, referral_id FROM bg3.users;"
    },
    {
        "table_name": "stg_referrals",
        "query": "SELECT id, name FROM bg3.referrals;"
    },
    {
        "table_name": "stg_tickets_status",
        "query": "SELECT id, description FROM bg3.ticket_status;"
    },
    {
        "table_name": "stg_tickets_categories",
        "query": "SELECT id, name FROM bg3.tickets_categories;"
    },
    {
        "table_name": "stg_tickets",
        "query": "SELECT id, created_at, first_response_at, resolved_at, ticket_category_id, user_id, ticket_status_id from bg3.tickets;"
    }
]
# Run EL
if __name__ == "__main__":
    for table in tables:
        extract_and_load_devices(table["table_name"], table["query"])
