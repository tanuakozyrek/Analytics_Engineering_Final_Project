from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, date
from sqlalchemy import create_engine, Table, MetaData, insert
from sqlalchemy.exc import SQLAlchemyError
import json
import os

# ---------------------------------------
# підключення
DB_URL = "..."
DB_SCHEMA = "fin_proj"
LOG_TABLE = "t_etl_logs"

# Шлях до dbt-проєкту всередині контейнера
PROJECT_PATH = "/usr/local/airflow/dags/final_project"
RESULT_PATH = os.path.join(PROJECT_PATH, "target", "run_results.json")

# ---------------------------------------
# логування dbt-результатів
def log_dbt_results():
    try:
        with open(RESULT_PATH, "r") as f:
            results = json.load(f)
    except Exception as e:
        raise RuntimeError(f"Неможливо відкрити run_results.json: {e}")

    records = []
    for result in results.get("results", []):
        unique_id = result.get("unique_id", "")
        model_name = unique_id.split(".")[-1] if unique_id else "unknown_model"
        rows = result.get("adapter_response", {}).get("rows_affected", 0)
        status = result.get("status", "UNKNOWN")
        runtime = result.get("execution_time", 0.0)
        msg = result.get("message", "")

        records.append({
            "etl_date": date.today(),
            "source_name": f"dbt_{model_name}",
            "records_loaded": rows,
            "status": status.upper(),
            "message": msg,
            "runtime_seconds": runtime
        })

    if not records:
        print("Жодного запису для логування. Можливо, dbt run завершився з помилкою.")
        return

    try:
        engine = create_engine(DB_URL)
        metadata = MetaData()
        metadata.reflect(bind=engine, schema=DB_SCHEMA)
        logs = Table(LOG_TABLE, metadata, schema=DB_SCHEMA, autoload_with=engine)

        with engine.begin() as conn:
            conn.execute(insert(logs), records)

        print(f"Записано {len(records)} логів у {DB_SCHEMA}.{LOG_TABLE}")
    except SQLAlchemyError as e:
        raise RuntimeError(f"Помилка при записі в базу: {e}")

# ---------------------------------------
# DAG
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="dbt_etl_ukrstat_dict",
    schedule="0 7 * * *",
    default_args=default_args,
    catchup=False,
    tags=["dbt", "ukrstat", "etl"],
) as dag:

    run_dbt_seed = BashOperator(
        task_id="run_dbt_seed_country_dict",
        bash_command=f"cd {PROJECT_PATH} && dbt seed --select country_dict --profiles-dir /home/astro/.dbt"
    )

    log_seed_results = PythonOperator(
        task_id="log_dbt_seed_results",
        python_callable=log_dbt_results
    )

    run_dbt = BashOperator(
        task_id="run_dbt_models",
        bash_command=f"cd {PROJECT_PATH} && dbt run --profiles-dir /home/astro/.dbt"
    )

    log_results = PythonOperator(
        task_id="log_dbt_run_results",
        python_callable=log_dbt_results
    )

    run_dbt_seed >> log_seed_results >> run_dbt >> log_results
