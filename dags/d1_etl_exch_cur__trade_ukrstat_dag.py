from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
import pandas as pd
import requests
from functools import wraps
from datetime import date

import os
import re
from bs4 import BeautifulSoup
from io import BytesIO
from pandas.tseries.offsets import MonthBegin

# 1. Підключення до БД
db_url = "..."
engine = create_engine(db_url)
#------------------------------------------------------------------------------------------------
# 2. Декоратор логування
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
# 3. завантаження курсу валют  - додавання кожен день, перезапис у разі повтору
@log_to_postgres("nbu_exchange_rate")
def save_exchange_rate():
    url = "https://bank.gov.ua/NBUStatService/v1/statdirectory/exchange?json"
    current_date = date.today()
    current_date_str = pd.to_datetime(current_date).strftime('%Y-%m-%d')

    response = requests.get(url)
    data = response.json()

    parsed_data = [{
        "currency_code": item["cc"],
        "currency_name": item["txt"],
        "rate": item["rate"],
        "date": current_date
    } for item in data]

    df = pd.DataFrame(parsed_data)
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()

    with engine.begin() as connection:
        connection.execute(text(f"DELETE FROM fin_proj.t1_raw_exch_rates WHERE date::date = '{current_date_str}'"))

    df.to_sql("t1_raw_exch_rates", schema="fin_proj", con=engine, if_exists="append", index=False)
    return df
#------------------------------------------------------------------------------------------------
#------------------------------------------------------------------------------------------------
# 4.завантаження даних по експорту/імпорту товарів - перезапис даних з поч.поточного року
@log_to_postgres(source_name="ukrstat_trade_data")
def save_trade_data():
    current_year = datetime.now().year
    page_url = f"https://www.ukrstat.gov.ua/operativ/operativ{current_year}/zd/kr_tstr/arh_kr_{current_year}.htm"

    def extract_period(filename):
        match = re.search(r"(\d{2})_(\d{4})", filename)
        if match:
            month, year = match.groups()
            return f"{year}-{month}"
        match = re.search(r"(\d{2})_(\d{2})", filename)
        if match:
            month, year = match.groups()
            return f"20{year}-{month}"
        return "unknown"

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

            df["source_file"] = filename
            df["period"] = extract_period(filename)
            df["year"] = df["period"].str[:4]
            #df["date"] = pd.to_datetime(df["period"]) + MonthBegin(1) #наступний місяць
            df["date"] = pd.to_datetime(df["period"] + "-01") #поточний місяць

            # логіка для group_label
            group_labels = []
            current_label = None
            for val in df.iloc[:, 0]:
                if pd.isna(val):
                    group_labels.append(None)
                    continue
                val_str = str(val).strip()
                if val_str and val_str[0].isdigit():
                    group_labels.append(current_label)
                else:
                    current_label = val_str
                    group_labels.append(None)
            df["group_label"] = group_labels

            dataframes.append(df)
            print(f"Завантажено: {filename}")
        except Exception as e:
            print(f"Помилка при обробці {url}: {e}")

    combined_df = pd.concat(dataframes, ignore_index=True)

    new_column_names = [
        "name_complex",
        "export_value_th_dol_usa",
        "export_percent_to_period_of_prev_year",
        "export_percent_of_total_volume",
        "import_value_th_dol_usa",
        "import_percent_to_period_of_prev_year",
        "import_percent_of_total_volume"
    ]
    columns = combined_df.columns.tolist()
    columns[:7] = new_column_names
    combined_df.columns = columns

    combined_df["product_name"] = combined_df["name_complex"].apply(
        lambda x: x if isinstance(x, str) and x.strip()[0].isdigit() else None
    )
    # Перетворюємо колонку на числову, нечислові значення стануть NaN
    combined_df["export_value_th_dol_usa"] = pd.to_numeric(
    combined_df["export_value_th_dol_usa"], errors="coerce"
    )

    # Видаляємо рядки, де значення NaN (тобто були текстом або порожні)
    combined_df = combined_df[combined_df["export_value_th_dol_usa"].notna()].copy()

    # Видаляємо записи поточного року
    with engine.begin() as connection:
        connection.execute(text(
            f"DELETE FROM fin_proj.t4_raw_ukrstat_trade_data WHERE year = '{current_year}'"
        ))

    # Завантаження у PostgreSQL
    combined_df.to_sql("t4_raw_ukrstat_trade_data", schema="fin_proj", con=engine, if_exists="append", index=False)
    return combined_df
#------------------------------------------------------------------------------------------------
#------------------------------------------------------------------------------------------------
# 5. Airflow DAG
with DAG(
    dag_id="exchange_currency_etl",
    description="Завантаження валютного курсу НБУ та товарної статистики з логуванням",
    schedule="@daily",
    start_date=datetime(2025, 7, 1),
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5)
    },
    tags=["nbu", "exchange_rate", "ukrstat", "etl"]
) as dag:

    task_exchange_rate = PythonOperator(
        task_id="load_daily_exchange_rate",
        python_callable=save_exchange_rate
    )

    task_trade_data = PythonOperator(
        task_id="load_trade_data_from_ukrstat",
        python_callable=save_trade_data
    )

    task_exchange_rate >> task_trade_data
