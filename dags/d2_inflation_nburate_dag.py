from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import date, datetime, timedelta
import pandas as pd
import requests
from sqlalchemy import create_engine
from functools import wraps

# 1.Підключення до PostgreSQL
db_url = "..."
engine = create_engine(db_url)
#--------------------------------------------------------------------------------------------------------------
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
#--------------------------------------------------------------------------------------------------------------
# 3. завантаження інфляції - перезапис даних
@log_to_postgres(source_name="minfin_inflation")
def save_inflation_data():
    url = "https://index.minfin.com.ua/ua/economy/index/inflation/"
    headers = {
        "User-Agent": "Mozilla/5.0"
    }
    resp = requests.get(url, headers=headers)
    tables = pd.read_html(resp.text)
    df_raw = tables[0]
    df_raw = df_raw.rename(columns={df_raw.columns[0]: "year"})
    if "За рік" in df_raw.columns:
        df_raw = df_raw.drop(columns=["За рік"])

    df_long = df_raw.melt(id_vars=["year"], var_name="month_ua", value_name="inflation")
    df_long["inflation"] = (
        df_long["inflation"]
        .astype(str)
        .str.replace(",", ".", regex=False)
        .str.replace(" ", "", regex=False)
        .astype(float)
        / 10
    )

    month_map = {
        "січень": 1, "лютий": 2, "березень": 3, "квітень": 4, "травень": 5,
        "червень": 6, "липень": 7, "серпень": 8, "вересень": 9,
        "жовтень": 10, "листопад": 11, "грудень": 12
    }
    df_long["month"] = df_long["month_ua"].map(month_map)
    df_long["date"] = pd.to_datetime(dict(year=df_long["year"], month=df_long["month"], day=1))
    df_long["date_s"] = df_long["date"].dt.strftime("%d.%m.%Y")
    df_long = df_long.sort_values("date").reset_index(drop=True)
    df_long = df_long[df_long["date"].dt.year == 2025]
    df_long = df_long.dropna(subset=["inflation"])

    df_long.to_sql("t2_raw_inflation_index", engine, schema="fin_proj", index=False, if_exists="replace")
    return df_long
#--------------------------------------------------------------------------------------------------------------
# 4.завантаження облікової ставки НБУ - перезапис даних
@log_to_postgres(source_name="nbu_rate")
def save_discount_rate():
    url = "https://index.minfin.com.ua/ua/banks/nbu/refinance/"
    headers = {"User-Agent": "Mozilla/5.0"}
    resp = requests.get(url, headers=headers)
    df = pd.read_html(resp.text)[0].copy()
    df.columns = ["period", "rate_pct", "change_pct"]
    df["date_start"] = pd.to_datetime(df["period"].str.extract(r"(\d{2}\.\d{2}\.\d{4})")[0], format="%d.%m.%Y", errors="coerce")
    df["rate_pct"] = df["rate_pct"].astype(str).str.replace(",", ".", regex=False).str.replace(" ", "", regex=False).astype(float)
    df = df.dropna(subset=["date_start"])
    df_2025 = df[df["date_start"].dt.year >= 2024].copy()

    calendar = pd.DataFrame({"date": pd.date_range("2025-01-01", "2025-12-31")})
    df_merge = df_2025[["date_start", "rate_pct"]].sort_values("date_start").rename(columns={"date_start": "date"})
    df_daily = pd.merge_asof(calendar.sort_values("date"), df_merge, on="date")
    df_daily["rate_pct"] = df_daily["rate_pct"] / 100
    df_daily = df_daily.dropna(subset=["rate_pct"])

    #yesterday = pd.to_datetime(date.today() - timedelta(days=1))
    #df_daily = df_daily[df_daily["date"] <= yesterday]

    df_daily.to_sql(
        name="t3_raw_nbu_rate",
        con=engine,
        index=False,
        if_exists="replace",
        schema="fin_proj"
    )
    return df_daily
#--------------------------------------------------------------------------------------------------------------
# 5.Airflow DAG
with DAG(
    dag_id="inflation_nburate_etl",
    description="Завантаження інфляції з Мінфін, облікової ставки з НБУ у PostgreSQL",
    start_date=datetime(2025, 7, 1),
    ### schedule_interval="@monthly",  # запуск щомісяця
    ### schedule_interval=timedelta(days=3),  # кожні 3 дні
    schedule="0 6 */3 * *",  # cron - кожні 3 дні о 6 ранку
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["etl", "inflation", "nbu", "rate"]
) as dag:

    load_inflation = PythonOperator(
        task_id='load_inflation_data',
        python_callable=save_inflation_data
    )

    load_discount_rate = PythonOperator(
        task_id='load_discount_rate',
        python_callable=save_discount_rate
    )

    #[load_inflation, load_discount_rate] - незалежний запуск
    load_inflation >> load_discount_rate
