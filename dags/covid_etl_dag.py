from airflow.decorators import dag, task    
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pandas as pd
import requests
import psycopg2
import os

# DAG default args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

@dag(
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['covid', 'etl']
)
def covid_etl():

    # ------------------------------
    # 1 Extract Task
    # ------------------------------
    @task
    def extract_covid_data():
        #Create local folders:
        raw_folder = '/opt/airflow/data/raw/'
        os.makedirs(raw_folder, exist_ok=True)
        
        #Download COVID-19 data from a public API:
        url = 'https://api.covidtracking.com/v1/us/daily.json'
        response = requests.get(url)
        data = response.json()

        #Convert to DataFrame and save as CSV:
        df = pd.DataFrame(data)
        df['date'] = pd.to_datetime(df['date'], format='%Y%m%d')

        #Save as CSV
        output_path = os.path.join(raw_folder, 'us_daily_covid.csv')
        df.to_csv(output_path, index=False)

        return output_path
    
   # ------------------------------
    # 2 Load Task
    # ------------------------------
    @task()
    def load_covid_data(csv_path: str):
        df = pd.read_csv(csv_path)

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
        CREATE TABLE IF NOT EXISTS raw.covid_data (
            date DATE NOT NULL,
            states VARCHAR(10),
            positive NUMERIC,
            negative NUMERIC,
            hospitalizedCurrently NUMERIC,
            death NUMERIC
        );
        """)
        conn.commit()

        # Truncate before insert
        cur.execute("TRUNCATE TABLE raw.covid_data;")
        conn.commit()

        # Insert rows
        for _, row in df.iterrows():
            cur.execute(
                """
                INSERT INTO raw.covid_data(date, states, positive, negative, hospitalizedCurrently, death)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (row['date'], row.get('states'), row.get('positive'), row.get('negative'),
                 row.get('hospitalizedCurrently'), row.get('death'))
            )
        conn.commit()
        cur.close()
        conn.close()

        return "Loaded successfully"

    # ------------------------------
    # 3 Transform Task (dbt)
    # ------------------------------
    run_dbt_transform = BashOperator(
    task_id="run_dbt_transform",
    bash_command="""
    cd /opt/airflow/dbt/covid_project &&
    dbt deps &&
    dbt run &&
    dbt test
    """
    )
   # ------------------------------
    # Task dependencies
    # ------------------------------
    csv_file = extract_covid_data()
    load_task = load_covid_data(csv_file)
    load_task >> run_dbt_transform

# Instantiate DAG
covid_etl()


