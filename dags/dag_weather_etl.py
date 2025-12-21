from airflow.decorators import dag, task
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import Variable
from datetime import timedelta, datetime

from include.openweather.src.missing_raw import identify_missing_dates
from include.openweather.src.extraction import get_day_summary
from include.openweather.src.transforming import parsing_daily_weather
from include.utils.db_interactors import execute_query, send_csv_df_to_db, move_files_after_loading
from include.utils.s3_cons import upload_all_files_to_s3

query_silver_upsert = """
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
    FROM raw.stg_openweather_daily
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

# Se der
# ALTER TABLE raw.openweather_daily
# 	ADD CONSTRAINT openweather_date_pk PRIMARY KEY (date);

query_daily_output = """
    SELECT * FROM raw.openweather_daily  
    """

query_kill_stg = """
    DROP TABLE raw.stg_openweather_daily;
    """


default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


@dag(
    dag_id="weather_etl",
    default_args=default_args,
    description="ETL for Weather Data",
    start_date=datetime(2025, 9, 21),
    schedule="55 23 * * *",
    catchup=False,
)
def weather_etl():
    staging_folder = "/usr/local/airflow/mylake/staging/weather_project/"       # ajuste se existir
    bronze_folder  = "/usr/local/airflow/mylake/bronze/weather_project/"

    # Sensor ainda é necessário no estilo clássico
    check_api_availability = HttpSensor(
        task_id="check_api",
        http_conn_id="openweather_conn",
        endpoint="data/3.0/onecall/day_summary",
        request_params={
            "lat": -23.137,
            "lon": -46.5547861,
            "date": "{{ ds }}",
            "appid":Variable.get("openweather_api"),
        },
        response_check=lambda response: response.status_code == 200,
        poke_interval=5,
        timeout=20,
    )

    @task.short_circuit
    def find_missing_dates():
        conn_id = 'postgres_dw'    
        # Step 1: Obtem a data máxima de cada tabela - Caso haja discrepância entre cargas
        db_hook = PostgresHook(postgres_conn_id=conn_id)
        return identify_missing_dates(db=db_hook)

    @task 
    def upload_to_s3():
        upload_all_files_to_s3(input_folder=staging_folder,
                            bucket_name="openweatherbrz",
                            prefix='control_file/',
                            con_id="aws_solar_weather",
                            sufix=".csv"
                            )


    @task
    def make_requests_to_api(list_of_dates:list):
        return get_day_summary(
            output_path=staging_folder,
            dates_list=list_of_dates,
            token=Variable.get("openweather_api"),
        )



    @task
    def transform():
        return parsing_daily_weather(staging_dir=staging_folder)
    
    @task 
    def load_staging(dataframe):
        send_csv_df_to_db(dataframe,table_name="stg_openweather_daily",schema="raw")
    
    @task 
    def merge_silver_table():
        execute_query(query_silver_upsert)

    @task 
    def clear_staging():
        move_files_after_loading(staging_folder, bronze_folder)

    @task
    def drop_staging():
        execute_query(query_kill_stg)

    t0 = check_api_availability
    t1 = find_missing_dates()
    t2 = upload_to_s3()
    t3 = make_requests_to_api(t1)
    t4 = transform()
    t5 = load_staging(t4)
    t6 = merge_silver_table()
    t7 = clear_staging()
    t8 = drop_staging()
    
    t0 >> t1 >> t3 >> t4 >> t5 >> t6 >> t7 >> t8
    t1 >> t2
    # t0 >> t1 >> t2 >> t4 >> t5 >> t6 >> t7 >> t8
    
dag = weather_etl()