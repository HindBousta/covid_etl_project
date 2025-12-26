from airflow.decorators import dag, task    
from datetime import datetime, timedelta
import pandas as pd
import requests
import psycopg2
from psycopg2.extras import execute_values
import os

# DAG default args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email': ['hindbousta6@gmail.com']
}

RAW_FOLDER = '/opt/airflow/data/raw/'

@dag(
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval='@weekly',
    catchup=False,
    tags=['census', 'etl']
)
def census_ingestion():
    
    # ------------------------------
    # 1 Extract Task
    # ------------------------------
    @task
    def extract_census_data():
        #Create local folders:
        os.makedirs(RAW_FOLDER, exist_ok=True)
        
        # Census ACS API for 2021 state-level demographics
        url = "https://api.census.gov/data/2021/acs/acs5?get=NAME,B01003_001E,B01002_001E&for=state:*"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        if not data:
            raise ValueError("No data fetched from Census API")

        #Convert to DataFrame and save as CSV:
        df = pd.DataFrame(data[1:], columns=data[0])
        df = df.rename(columns={
            'state': 'state_fips',
            'NAME': 'state_name',
            'B01003_001E': 'total_population',
            'B01002_001E': 'median_age'
        })

        # Add ingestion_date
        df['ingestion_date'] = pd.to_datetime(datetime.now().date())
        
        #Save as CSV
        output_path = os.path.join(RAW_FOLDER, 'census_data.csv')
        df.to_csv(output_path, index=False)

        return {"path": output_path, "rows": len(df)}
    
    # ------------------------------
    # 2 Validate Task
    # ------------------------------
    @task()
    def validate_data(metadata: dict):
        if metadata["rows"] == 0:
            raise ValueError("Extracted dataset is empty")
        
        df = pd.read_csv(metadata["path"])
        expected_columns = {'state_name', 'total_population', 'median_age', 'state_fips', 'ingestion_date'}
        if not expected_columns.issubset(set(df.columns)):
            raise ValueError("Extracted dataset is missing required columns")
        
        return metadata["path"]

    # ------------------------------
    # 3 Load Task
    # ------------------------------
    @task()
    def load_census_data(csv_path: str):
        df = pd.read_csv(csv_path)

        # Convert numeric columns to native Python types
        df['total_population'] = df['total_population'].apply(lambda x: int(x) if pd.notnull(x) else None)
        df['median_age'] = df['median_age'].apply(lambda x: float(x) if pd.notnull(x) else None)

        # Connect to Postgres
        conn = psycopg2.connect(
            host=os.environ.get('POSTGRES_HOST', 'postgres'),
            database=os.environ.get('POSTGRES_DB', 'covid_db'),
            user=os.environ.get('POSTGRES_USER', 'airflow'),
            password=os.environ.get('POSTGRES_PASSWORD', 'airflow')
        )
        cur = conn.cursor()

        # Create schema/table if not exists
        cur.execute("""
        CREATE SCHEMA IF NOT EXISTS raw;
        DROP TABLE IF EXISTS raw.census_data;
        CREATE TABLE IF NOT EXISTS raw.census_data (
            state_fips VARCHAR(20) PRIMARY KEY,
            state_name VARCHAR(100),
            total_population NUMERIC,
            median_age NUMERIC,
            ingestion_date DATE
        );
        """)
        conn.commit()

        # Truncate before insert
        cur.execute("TRUNCATE TABLE raw.census_data;")
        conn.commit()

        # Bulk Insert rows
        records = list(df.itertuples(index=False, name=None))
        execute_values(
            cur,
            """
            INSERT INTO raw.census_data (state_fips, state_name, total_population, median_age, ingestion_date)
            VALUES %s
            """,
            records)
        conn.commit()
        cur.close()
        conn.close()

        return f"Loaded {len(df)} rows successfully"
    
    # ------------------------------
    # DAG task dependencies
    # ------------------------------

    extracted_data = extract_census_data()
    validated_data = validate_data(extracted_data)
    load_census_data(validated_data)

# Instantiate DAG
census_ingestion()