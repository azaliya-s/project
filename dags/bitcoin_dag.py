import requests
from google.cloud import bigquery
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.python import PythonOperator 
from datetime import datetime
import json

# Constants
PROJECT_ID = 'bitcoin-438011'  # Replace with your GCP project ID
DATASET_ID = 'raw_dataset'  # BigQuery Dataset
TABLE_ID = 'raw_bitcoin_data'  # BigQuery Table

# Define the default_args dictionary for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 12, 6),
    'retries': 1,
}

# Initialize the DAG
with DAG(
    'bitcoin_data_ingest',
    default_args=default_args,
    description='A DAG to fetch Bitcoin data and load it into BigQuery',
    schedule_interval='@daily',  # Adjust the schedule as needed
    catchup=False,
) as dag:

    # Task 1: Extract Bitcoin data from the CoinGecko API (last 1 day)
    def extract_bitcoin_data():
        url = 'https://api.coingecko.com/api/v3/coins/bitcoin/market_chart?vs_currency=usd&days=1'
        response = requests.get(url)
        data = response.json()
        # Log data to debug (you can remove this in production)
        print(json.dumps(data, indent=4))
        return data

    extract_data = PythonOperator(
        task_id='extract_bitcoin_data',
        python_callable=extract_bitcoin_data,
    )

    # Task 2: Transform the data (this is optional for now, as we directly load it)
    def transform_data(data):
        # Flatten the data structure as needed (if necessary)
        transformed_data = []
        for price_data in data['prices']:
            timestamp = price_data[0]
            price = price_data[1]
            transformed_data.append({
                'timestamp': datetime.utcfromtimestamp(timestamp / 1000),
                'price_usd': price,
            })
        return transformed_data

    def load_data_to_bigquery(data):
        client = bigquery.Client(project= 'bitcoin-438011')
        dataset_ref = client.dataset('bitcoin-438011.raw_dataset')
        table_ref = dataset_ref.table(TABLE_ID)

        # Define the schema of the raw table
        schema = [
            bigquery.SchemaField('timestamp', 'TIMESTAMP'),
            bigquery.SchemaField('price_usd', 'FLOAT'),
        ]

        # Create the BigQuery table if it does not exist
        try:
            client.get_table(table_ref)  # Try to fetch the table
        except:
            # If the table does not exist, create it
            client.create_table(bigquery.Table(table_ref, schema=schema))

        # Insert data into the table
        rows_to_insert = transform_data(data)
        errors = client.insert_rows_json(table_ref, rows_to_insert)
        if errors == []:
            print(f'Successfully inserted {len(rows_to_insert)} rows')
        else:
            print(f'Error inserting rows: {errors}')

    # Task 3: Load data into BigQuery
    load_data = PythonOperator(
        task_id='load_bitcoin_data',
        python_callable=load_data_to_bigquery,
        op_args=[extract_data.output],
    )

    # Set task dependencies
    extract_data >> load_data
