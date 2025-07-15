from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import requests
from bs4 import BeautifulSoup
from io import BytesIO
import os
from sqlalchemy import create_engine, text
from functools import wraps

# 1.Підключення
db_url = "postgresql://tanykozyr2025:foS_*529gpx@postgresql-tanykozyr2025.alwaysdata.net:5432/tanykozyr2025_postsql"
engine = create_engine(db_url)
#------------------------------------------------------------------------------------------------
# 2.Декоратор логування
def log_to_postgres(source_name: str):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = datetime.now()
            try:
                df_result = func(*args, **kwargs)
                status = "success"
                message = ""
                records_loaded = len(df_result)
            except Exception as e:
                df_result = None
                status = "error"
                message = str(e)
                records_loaded = 0

            runtime = (datetime.now() - start_time).total_seconds()
            log_df = pd.DataFrame([{
                "etl_date": start_time.date(),
                "source_name": source_name,
                "records_loaded": records_loaded,
                "status": status,
                "message": message,
                "runtime_seconds": runtime
            }])
            log_df.to_sql("t_etl_logs", engine, schema="fin_proj", index=False, if_exists="append")

            if status == "error":
                raise Exception(message)
            return df_result
        return wrapper
    return decorator
#------------------------------------------------------------------------------------------------
# 3.завантаження RAW довідника по товарним групам - перезапис даних з поч.поточного року
@log_to_postgres("ukrstat_trade_dictionary")
def save_trade_dictionary():
    current_year = datetime.now().year
    page_url = f"https://www.ukrstat.gov.ua/operativ/operativ{current_year}/zd/tsztt/arh_tsztt{current_year}_u.html"

    response = requests.get(page_url)
    soup = BeautifulSoup(response.content, "html.parser")

    excel_links = []
    for link in soup.find_all("a", href=True):
        href = link["href"]
        if href.endswith(".xls") or href.endswith(".xlsx"):
            full_url = requests.compat.urljoin(page_url, href)
            excel_links.append(full_url)

    dataframes = []
    for url in excel_links:
        file_response = requests.get(url)
        filename = os.path.basename(url)
        try:
            if filename.endswith(".xls"):
                df = pd.read_excel(BytesIO(file_response.content), engine="xlrd")
            else:
                df = pd.read_excel(BytesIO(file_response.content), engine="openpyxl")

            df.columns.values[0] = "name_complex"
            df["source_file"] = filename
            df["year"] = current_year
            dataframes.append(df)
        except Exception as e:
            print(f"⚠️ Помилка при обробці {url}: {e}")

    combined_df = pd.concat(dataframes, ignore_index=True)

    group_names = []
    current_group = None
    for val in combined_df.iloc[:, 0]:
        if pd.isna(val):
            group_names.append(None)
            continue
        val_str = str(val).strip()
        if val_str and not val_str[0].isdigit():
            current_group = val_str
            group_names.append(None)
        else:
            group_names.append(current_group)

    combined_df["group_name"] = group_names
    columns = combined_df.columns.tolist()
    columns[0] = "name_complex"
    if len(columns) >= 8:
        columns[7] = "name_complex_eng"
    combined_df.columns = columns

    dict_df = combined_df.iloc[:, [0, 7, 8, 9, -1]].copy()

    # Очистка таблиці
    with engine.begin() as connection:
        connection.execute(text(
            f"DELETE FROM fin_proj.t5_raw_ukrstat_trade_dict WHERE year = '{current_year}'"
        ))

    # Завантаження в PostgreSQL
    dict_df.to_sql("t5_raw_ukrstat_trade_dict", schema="fin_proj", con=engine, if_exists="append", index=False)
    return dict_df

# 4.Airflow DAG
with DAG(
    dag_id="trade_dictionary_etl",
    description="Завантаження довідника товарних груп з ukrstat.gov.ua",
    schedule="0 6 2-31/2 * *",  # кожні два дні
    start_date=datetime(2025, 7, 1),
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5)
    },
    tags=["ukrstat", "trade", "dict", "etl"]
) as dag:

    task_save_trade_dict = PythonOperator(
        task_id="load_trade_dictionary",
        python_callable=save_trade_dictionary
    )