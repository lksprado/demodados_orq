from airflow.decorators import dag, task
from datetime import datetime

from include.Solar.src.transforming import (
    parsing_json_to_dataframe,
    make_hourly_df,
    make_daily_summary_df
)
from include.utils.db_interactors import send_csv_df_to_db, execute_query



query_hourly = """
    CREATE TABLE IF NOT EXISTS raw.solar_hourly_energy (
        datetime TEXT UNIQUE,
        energy TEXT
    );
"""

query_daily = """
    CREATE TABLE IF NOT EXISTS raw.solar_daily_energy (
        date TEXT UNIQUE,
        duration TEXT,
        total TEXT,
        co2 TEXT,
        max TEXT
    );
"""

query_merge_hourly = """
    INSERT INTO raw.solar_hourly_energy (datetime, energy)
    SELECT datetime, energy FROM raw.stg_solar_hourly_energy
    ON CONFLICT (datetime) DO UPDATE SET
    energy = EXCLUDED.energy;
    """

query_merge_daily = """
    INSERT INTO raw.solar_daily_energy (date, duration, total, co2, max)
    SELECT date, duration, total, co2, max FROM raw.stg_solar_daily_energy
    ON CONFLICT (date) DO UPDATE SET
    duration = EXCLUDED.duration,
    total = EXCLUDED.total,
    co2 = EXCLUDED.co2,
    max = EXCLUDED.max;
    """

query_kill_stg_daily = """
    DROP TABLE raw.stg_solar_daily_energy;
    """

query_kill_stg_hourly = """
    DROP TABLE raw.stg_solar_hourly_energy
    """

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 9, 21),
    "retries": 0,
}

@dag(
    dag_id="solar_etl_full",
    default_args=default_args,
    description="ETL for Solar Data Recovery",
    schedule=None,
    catchup=False,
)
def solar_etl_full():
    INPUT_FOLDER = "/opt/airflow/files/bronze/solar_project/"

    @task
    def create_raw_table_hourly():
        execute_query(query_hourly)

    @task
    def create_raw_table_daily():
        execute_query(query_daily)

    @task
    def parse_json_to_df():
        return parsing_json_to_dataframe(staging_dir=INPUT_FOLDER)

    @task
    def make_hourly(df):
        return make_hourly_df(df)

    @task
    def make_daily(df):
        return make_daily_summary_df(df)

    @task
    def load_hourly_staging(df):
        send_csv_df_to_db(df, "stg_solar_hourly_energy", "raw")

    @task
    def load_daily_staging(df):
        send_csv_df_to_db(df, "stg_solar_daily_energy", "raw")


    @task
    def merge_raw_table_hourly():
        execute_query(query_merge_hourly)
        

    @task
    def merge_raw_table_daily():
        execute_query(query_merge_daily)

    @task
    def drop_staging_daily():
        execute_query(query_kill_stg_daily)

    @task
    def drop_staging_hourly():
        execute_query(query_kill_stg_hourly)


    df = parse_json_to_df()
    df_hourly = make_hourly(df)
    df_daily = make_daily(df)

    load_hourly_staging(df_hourly) >> create_raw_table_hourly() >> merge_raw_table_hourly() >> drop_staging_hourly()
    load_daily_staging(df_daily) >> create_raw_table_daily() >> merge_raw_table_daily() >> drop_staging_daily()

dag = solar_etl_full()
