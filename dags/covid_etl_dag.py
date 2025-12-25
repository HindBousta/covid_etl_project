from airflow.decorators import dag, task    
from datetime import datetime, timedelta
import pandas as pd
import requests
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
)
def covid_etl():
    @task
    def extract_covid_data():
        #Creaye local folders:
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
    
    csv_dataset = extract_covid_data()

#Allow the DAG to be run:
covid_etl()
