from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "flex-eau",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="flex_eau_pipeline",
    default_args=default_args,
    description="Pipeline ETL complet Flex Eau",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["flex-eau", "etl", "dwh"],
) as dag:

    # ==================================================
    # INGESTION
    # ==================================================

    ingestion = BashOperator(
        task_id="ingestion_mysql_staging",
        cwd="/opt/project",
        bash_command="""
        set -e
        python ingestion/mysql_to_staging.py
        """,
    )

    # ==================================================
    # DIMENSIONS
    # ==================================================

    dim_date = BashOperator(
        task_id="dim_date",
        cwd="/opt/project",
        bash_command="python spark_jobs/dimensions/dim_date.py",
    )

    dim_geo = BashOperator(
        task_id="dim_geo",
        cwd="/opt/project",
        bash_command="python spark_jobs/dimensions/dim_geo.py",
    )

    dim_forage = BashOperator(
        task_id="dim_forage",
        cwd="/opt/project",
        bash_command="python spark_jobs/dimensions/dim_forage.py",
    )

    dim_usage = BashOperator(
        task_id="dim_usage",
        cwd="/opt/project",
        bash_command="python spark_jobs/dimensions/dim_usage.py",
    )

    dim_abonne = BashOperator(
        task_id="dim_abonne",
        cwd="/opt/project",
        bash_command="python spark_jobs/dimensions/dim_abonne.py",
    )

    dim_mode_paiement = BashOperator(
        task_id="dim_mode_paiement",
        cwd="/opt/project",
        bash_command="python spark_jobs/dimensions/dim_mode_paiement.py",
    )

    dim_type_incident = BashOperator(
        task_id="dim_type_incident",
        cwd="/opt/project",
        bash_command="python spark_jobs/dimensions/dim_type_incident.py",
    )

    dim_compteur = BashOperator(
        task_id="dim_compteur",
        cwd="/opt/project",
        bash_command="python spark_jobs/dimensions/dim_compteur.py",
    )

    # ==================================================
    # FACTS
    # ==================================================

    fact_production = BashOperator(
        task_id="fact_production",
        cwd="/opt/project",
        bash_command="python spark_jobs/facts/fact_production.py",
    )

    fact_energie = BashOperator(
        task_id="fact_energie",
        cwd="/opt/project",
        bash_command="python spark_jobs/facts/fact_energie.py",
    )

    fact_facturation = BashOperator(
        task_id="fact_facturation",
        cwd="/opt/project",
        bash_command="python spark_jobs/facts/fact_facturation.py",
    )

    fact_paiement = BashOperator(
        task_id="fact_paiement",
        cwd="/opt/project",
        bash_command="python spark_jobs/facts/fact_paiement.py",
    )

    fact_exploitation = BashOperator(
        task_id="fact_exploitation",
        cwd="/opt/project",
        bash_command="python spark_jobs/facts/fact_exploitation.py",
    )

    # ==================================================
    # DEPENDANCES
    # ==================================================

    dimensions = [
        dim_date,
        dim_geo,
        dim_forage,
        dim_usage,
        dim_abonne,
        dim_mode_paiement,
        dim_type_incident,
        dim_compteur,
    ]

    facts = [
        fact_production,
        fact_energie,
        fact_facturation,
        fact_paiement,
        fact_exploitation,
    ]

    ingestion >> dimensions

    for dim in dimensions:
        dim >> facts