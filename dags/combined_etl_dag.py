from airflow.decorators import dag,task
from airflow.utils.task_group import TaskGroup
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

from tasks.covid_tasks import extract_covid_data, validate_covid_data, load_covid_data
from tasks.census_tasks import extract_census_data, validate_census_data, load_census_data

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email': ['hindbousta6@gmail.com']
}

@dag(
    dag_id='combined_etl_dag',
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['etl', 'combined', 'dbt']
)

def combined_etl_dag():

    # ---------------------------
    # Covid Task Group
    # ---------------------------
    with TaskGroup('covid_ingestion_group') as covid_group:
        @task
        def extract_covid():
            return extract_covid_data()

        @task
        def validate_covid(data):
            return validate_covid_data(data)

        @task
        def load_covid(data):
            return load_covid_data(data)

        covid_data = extract_covid()
        validated_covid_data = validate_covid(covid_data)
        covid_chain = load_covid(validated_covid_data)
    # ---------------------------
    # Census Task Group
    # ---------------------------
    with TaskGroup('census_ingestion_group') as census_group:
        @task
        def extract_census():
            return extract_census_data()

        @task
        def validate_census(data):
            return validate_census_data(data)

        @task
        def load_census(data):
            return load_census_data(data)

        census_data = extract_census()
        validated_census_data = validate_census(census_data)    
        census_chain = load_census(validated_census_data)
    # ---------------------------
    # DBT Task Group
    # ---------------------------
    with TaskGroup('dbt_transform_group') as dbt_group:
        run_dbt_task = BashOperator(
            task_id="run_dbt_transform",
            bash_command="""
            cd /opt/airflow/dbt/covid_project &&
            dbt deps &&
            dbt run &&
            dbt test
            """
        )

    # -----------------------
    # DAG dependencies
    # -----------------------
    [covid_chain, census_chain] >> run_dbt_task


#Initiate the DAG
combined_etl_dag()