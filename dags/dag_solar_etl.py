from airflow.decorators import dag, task
from pendulum import datetime
from airflow.sdk import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

from pathlib import Path

# -----------------------------
# Imports do projeto
# -----------------------------
from include.Solar.src.missing_raw import identify_missing_dates

from include.Solar.src.extraction import (
    SeleniumConfig,
    Credentials,
    PathsConfig,
    run_pipeline,
)

from include.Solar.src.transforming import (
    parsing_json_to_dataframe,
    make_hourly_df,
    make_daily_summary_df
)

from include.utils.s3_cons import upload_all_files_to_s3
from include.utils.db_interactors import (
    execute_query,
    send_csv_df_to_db,
    move_files_after_loading
)

# -----------------------------
# SQL
# -----------------------------
query_merge_stg_hourly_to_raw = """
INSERT INTO raw.solar_hourly_energy (datetime, energy)
SELECT datetime, energy FROM raw.stg_solar_hourly_energy
ON CONFLICT (datetime) DO UPDATE SET
energy = EXCLUDED.energy;
"""

query_merge_stg_daily_to_raw = """
INSERT INTO raw.solar_daily_energy (date, duration, total, co2, max)
SELECT date, duration, total, co2, max FROM raw.stg_solar_daily_energy
ON CONFLICT (date) DO UPDATE SET
duration = EXCLUDED.duration,
total = EXCLUDED.total,
co2 = EXCLUDED.co2,
max = EXCLUDED.max;
"""

query_kill_stg_daily = "DROP TABLE raw.stg_solar_daily_energy;"
query_kill_stg_hourly = "DROP TABLE raw.stg_solar_hourly_energy;"

# -----------------------------
# DAG CONFIG
# -----------------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 9, 21),
    "retries": 0,
}

@dag(
    dag_id="solar_etl",
    default_args=default_args,
    description="ETL for Solar Data",
    schedule="55 23 * * *",
    catchup=False,
    tags=["atibaia"]
)
def solar_etl():

    # -----------------------------
    # CONFIGURAÇÕES INJETADAS
    # -----------------------------
    creds = Credentials(
        username=Variable.get("apsystem_user"),
        password=Variable.get("apsystem_pw"),
    )

    selenium_config = SeleniumConfig(
        remote_url="http://host.docker.internal:4444/wd/hub",
        headless=True,
    )

    staging_folder = Path("/usr/local/airflow/mylake/staging/solar_project/")
    bronze_folder = Path("/usr/local/airflow/mylake/bronze/solar_project/")

    paths = PathsConfig(
        staging_dir=staging_folder
    )

    # -----------------------------
    # TASKS
    # -----------------------------
    @task.short_circuit
    def find_missing_dates():
        db_hook = PostgresHook(postgres_conn_id="postgres_dw")
        return identify_missing_dates(db=db_hook)

    @task
    def extraction_json_files(list_of_dates: list):
        run_pipeline(
            selenium_config=selenium_config,
            creds=creds,
            paths=paths,
            dates=list_of_dates,
        )

    @task
    def upload_json_to_s3():
        upload_all_files_to_s3(
            input_folder=str(staging_folder),
            bucket_name="solar-weather",
            con_id="aws_solar_weather",
        )

    @task
    def parse_json_to_df():
        return parsing_json_to_dataframe(
            staging_dir=str(staging_folder)
        )

    @task
    def make_hourly_csv(csv_file):
        return make_hourly_df(csv_file)

    @task
    def make_daily_csv(csv_file):
        return make_daily_summary_df(csv_file)

    @task
    def load_hourly_to_staging(csv_file):
        send_csv_df_to_db(csv_file, "stg_solar_hourly_energy", "raw")

    @task
    def load_daily_to_staging(csv_file):
        send_csv_df_to_db(csv_file, "stg_solar_daily_energy", "raw")

    @task
    def merge_raw_table_hourly():
        execute_query(query_merge_stg_hourly_to_raw)

    @task
    def merge_raw_table_daily():
        execute_query(query_merge_stg_daily_to_raw)

    @task
    def clear_staging_dir():
        move_files_after_loading(
            str(staging_folder),
            str(bronze_folder)
        )

    @task
    def drop_staging_tables():
        execute_query(query_kill_stg_daily)
        execute_query(query_kill_stg_hourly)

    # -----------------------------
    # FLUXO
    # -----------------------------
    missing_dates = find_missing_dates()

    extraction = extraction_json_files(missing_dates)

    upload = upload_json_to_s3()
    parsed_df = parse_json_to_df()

    hourly_df = make_hourly_csv(parsed_df)
    daily_df = make_daily_csv(parsed_df)

    hourly_flow = (
        load_hourly_to_staging(hourly_df)
        >> merge_raw_table_hourly()
    )

    daily_flow = (
        load_daily_to_staging(daily_df)
        >> merge_raw_table_daily()
    )

    extraction >> upload
    extraction >> parsed_df

    upload >> [hourly_flow, daily_flow]
    [hourly_flow, daily_flow] >> clear_staging_dir() >> drop_staging_tables()


# Instancia a DAG
dag = solar_etl()
