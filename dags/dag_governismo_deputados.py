import logging
from datetime import datetime

import pandas as pd
from airflow.decorators import dag, task

from src.utils.pipeline_cfg import PipelineConfig, GenericETL
from src.pipelines.legislativo.radar_congresso import transform_governismo
from src.pipelines.legislativo.schema import GovernismoSchema
from src.utils.loaders.postgres import PostgreSQLManager
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger_g = logging.getLogger("DAG: governismo")


# Configuração (pode vir de .env, Airflow Variables, etc.)
PIPELINE_GOVERNISMO_DEPUTADOS_CONFIG_PRD = {
    "url_base": "https://radar.congressoemfoco.com.br/api/governismo?casa=camara",
    "landing_dir": "/usr/local/airflow/mylake/raw/radar_congresso/governismo/",
    "landing_file": "radar_governismo_deputados.json",
    "bronze_dir": "/usr/local/airflow/mylake/bronze/radar_congresso/governismo/",
    "bronze_file": "radar_governismo_deputados.csv",
    "db_table": "stg_radar_governismo_deputados",
}


@dag(
    dag_id="governismo_deputados_pipeline",
    start_date=datetime(2025, 10, 25),
    schedule="@weekly",
    catchup=False,
    tags=["radar_congresso"],
)
def governismo_pipeline():
    hook = PostgresHook(postgres_conn_id="demodadosdw")
    engine = hook.get_sqlalchemy_engine()
    pg = PostgreSQLManager(engine=engine)  # usa engine externa
    # Instancia o ETL genérico
    cfg = PipelineConfig(**PIPELINE_GOVERNISMO_DEPUTADOS_CONFIG_PRD)
    etl = GenericETL(
        cfg=cfg,
        extract_fn=None,
        transform_fn=transform_governismo,
        validate_fn=None,
        load_fn=pg,
        validator=GovernismoSchema,
        log=logger_g,
    )

    @task
    def extract():
        etl.extract()
        return str(etl.cfg.landing_filepath)

    @task
    def transform(landing_path: str):
        df = pd.read_json(landing_path)
        transformed = etl.transform(df)
        transformed.to_csv(etl.cfg.bronze_filepath, index=False)
        return str(etl.cfg.bronze_filepath)

    @task
    def validate(bronze_path: str):
        df = pd.read_csv(bronze_path)
        validated = etl.validate(df)
        validated.to_csv(bronze_path, index=False)
        return bronze_path



    @task
    def load_staging(validated_path: str):
        df = pd.read_csv(validated_path, dtype=str)
        pg.send_df_to_db(df, table_name=etl.cfg.db_table, how="replace")

    @task
    def check_staging_count():
        result = pg.fetchone(f"SELECT COUNT(*) FROM raw.{etl.cfg.db_table}")
        if not result or result[0] == 0:
            raise ValueError("Staging está vazia, abortando promoção para raw")
        logger_g.info(f"✅ Staging tem {result[0]} linhas")

    @task
    def create_raw_new():
        pg.execute_query(f"""
            DROP TABLE IF EXISTS raw.raw_radar_governismo_deputados_new;
            CREATE TABLE raw.raw_radar_governismo_deputados_new AS
            SELECT * FROM raw.{etl.cfg.db_table};
        """)

    @task
    def swap_raw():
        pg.execute_query("""
            DROP TABLE IF EXISTS raw.raw_radar_governismo_deputados;
            ALTER TABLE raw.raw_radar_governismo_deputados_new
            RENAME TO raw_radar_governismo_deputados;
        """)

    @task
    def drop_stg_if_exists():
        pg.execute_query(f"""
            DROP TABLE IF EXISTS raw.{etl.cfg.db_table};
        """)

    # ---------- Encadeamento ----------
    raw = extract()
    transformed = transform(raw)
    validated = validate(transformed)

    (
        load_staging(validated)
        >> check_staging_count()
        >> create_raw_new()
        >> swap_raw()
        >> drop_stg_if_exists()
    )

# 👇 necessário para o Airflow reconhecer a DAG
dag = governismo_pipeline()