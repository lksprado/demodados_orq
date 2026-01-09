from airflow import DAG
from datetime import timedelta, datetime
from airflow.decorators import dag, task
from include.openweather.src.transforming import parsing_daily_weather
from include.utils.db_interactors import execute_query, send_csv_df_to_db


default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

query_raw_ddl = """
    CREATE TABLE IF NOT EXISTS raw.openweather_daily (
        date TEXT UNIQUE,
        cloud_cover_afternoon TEXT,
        humidity_afternoon TEXT,
        precipitation_total TEXT,
        temperature_min TEXT,
        temperature_max TEXT,
        temperature_afternoon TEXT,
        temperature_night TEXT,
        temperature_evening TEXT,
        temperature_morning TEXT,
        pressure_afternoon TEXT,
        wind_max_speed TEXT,
        wind_max_direction TEXT
    );
"""


query_raw_upsert = """
    INSERT INTO raw.openweather_daily (
        date,
        cloud_cover_afternoon,
        humidity_afternoon,
        precipitation_total,
        temperature_min,
        temperature_max,
        temperature_afternoon,
        temperature_night,
        temperature_evening,
        temperature_morning,
        pressure_afternoon,
        wind_max_speed,
        wind_max_direction
        )
    SELECT
        date,
        cloud_cover_afternoon,
        humidity_afternoon,
        precipitation_total,
        temperature_min,
        temperature_max,
        temperature_afternoon,
        temperature_night,
        temperature_evening,
        temperature_morning,
        pressure_afternoon,
        wind_max_speed,
        wind_max_direction
    FROM staging.stg_openweather__atb_daily
    ON CONFLICT (date) DO UPDATE SET
        cloud_cover_afternoon = EXCLUDED.cloud_cover_afternoon,
        humidity_afternoon = EXCLUDED.humidity_afternoon,
        precipitation_total = EXCLUDED.precipitation_total,
        temperature_min = EXCLUDED.temperature_min,
        temperature_max = EXCLUDED.temperature_max,
        temperature_afternoon = EXCLUDED.temperature_afternoon,
        temperature_night = EXCLUDED.temperature_night,
        temperature_evening = EXCLUDED.temperature_evening,
        temperature_morning = EXCLUDED.temperature_morning,
        pressure_afternoon = EXCLUDED.pressure_afternoon,
        wind_max_speed = EXCLUDED.wind_max_speed,
        wind_max_direction = EXCLUDED.wind_max_direction;
    """
    
query_kill_stg = """
    DROP TABLE staging.stg_openweather_daily;
    """


@dag(
    dag_id='weather_etl_full',
    default_args=default_args,
    description='Fetch and process weather data from OpenWeather API',
    start_date= datetime(2025, 9, 21),
    schedule=None,
    catchup=False,
    tags=["atibaia"]
)

def weather_etl_full():
    INPUT_FOLDER = '/opt/airflow/files/bronze/weather_project/'


    @task
    def create_silver_table():
        execute_query(query_raw_ddl)

    @task
    def parse_json_to_df():
        return parsing_daily_weather(staging_dir=INPUT_FOLDER)

    @task
    def load_staging(df):
        send_csv_df_to_db(df, "stg_openweather_daily", "raw")

    @task
    def merge_raw_table():
        execute_query(query_raw_upsert)

    @task
    def kill_staging():
        execute_query(query_kill_stg)

    df = parse_json_to_df()
    load_staging(df) >> create_silver_table() >> merge_raw_table() >> kill_staging()

dag = weather_etl_full()
