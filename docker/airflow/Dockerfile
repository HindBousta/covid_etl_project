FROM apache/airflow:2.7.3-python3.10

USER root

#System dependencies (git is needed by dbt deps)
RUN apt-get update && apt-get install -y \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

#Install dbt and the needed plugins
RUN pip install --no-cache-dir dbt-postgres==1.9.1