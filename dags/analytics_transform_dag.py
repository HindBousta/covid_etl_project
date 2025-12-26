from airflow.decorators import dag, task    
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta

# DAG default args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email': ['hindbousta6@gmail.com']
}

RAW_FOLDER = '/opt/airflow/data/raw/'

@dag(
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['dbt', 'analytics']
)
def dbt_run_dag():
    
    #Wait for Covid Data Ingestion DAG to complete
    wait_covid = ExternalTaskSensor(
        task_id='wait_for_covid_data_ingestion',
        external_dag_id='covid_ingestion',
        external_task_id=None,
        poke_interval=300,
        timeout=600,
        mode='poke'
    )
    
    #Wait for CENSUS Data Ingestion DAG to complete
    wait_census = ExternalTaskSensor(
        task_id='wait_for_census_data_ingestion',
        external_dag_id='census_ingestion',
        external_task_id=None,
        poke_interval=300,
        timeout=600,
        mode='poke'
    )

    # ------------------------------
    # Transform Task (dbt)
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

    # Task dependencies
    [wait_covid, wait_census] >> run_dbt_transform

# Instantiate DAG
dbt_run_dag()


