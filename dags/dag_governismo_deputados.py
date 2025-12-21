import logging
from datetime import datetime

import pandas as pd
from airflow.decorators import dag, task

from include.local_setup.src.utils.pipeline_cfg import PipelineConfig, GenericETL
from include.local_setup.src.pipelines.legislativo.radar_governismo import transform_governismo
from include.local_setup.src.pipelines.legislativo.schema import GovernismoSchema
from include.local_setup.src.utils.loaders.postgres import PostgreSQLManager
from airflow.providers.postgres.hooks.postgres import PostgresHook




@dag(
    dag_id="governismo_deputados_pipeline",
    start_date=datetime(2025, 10, 24),
    schedule="@weekly",
    catchup=False,
    tags=["radar_congresso"],
)
def governismo_pipeline():
    logger_g = logging.getLogger("DAG: governismo")


    # Configuração (pode vir de .env, Airflow Variables, etc.)
    PIPELINE_GOVERNISMO_DEPUTADOS_CONFIG_PRD = {
        "url_base": "https://radar.congressoemfoco.com.br/api/governismo?casa=camara",
        "landing_dir": "/usr/local/airflow/mylake/raw/demodados/radar_congresso/governismo/",
        "landing_file": "radar_governismo_deputados.json",
        "bronze_dir": "/usr/local/airflow/mylake/bronze/demodados/radar_congresso/governismo/",
        "bronze_file": "radar_governismo_deputados.csv",
        "db_table": "stg_radar_governismo_deputados",
    }

    target = 'raw_radar_governismo_deputados'
    
    # Hook/engine do Postgres para dentro da DAG
    hook = PostgresHook(postgres_conn_id="demodadosdw")
    engine = hook.get_sqlalchemy_engine()
    pg = PostgreSQLManager(engine=engine)  # usa engine do Airflow

    # Instanciações
    cfg = PipelineConfig(**PIPELINE_GOVERNISMO_DEPUTADOS_CONFIG_PRD)
    etl = GenericETL(
        cfg=cfg,
        extract_fn=None,
        load_fn=None,
        validator=GovernismoSchema,
        log=logger_g,
    )

    # ------- Tasks (cada uma usa somente cfg/etl) -------
    @task
    def t_extract():
        etl.extract()

    @task
    def t_transform():
        transform_governismo(cfg)

    # @task
    # def t_validate():
    #     etl.validate()

    @task
    def t_create_schema():
        pg.execute_query(f"CREATE SCHEMA IF NOT EXISTS raw")

    @task
    def t_load_staging():
        pg.execute_query(f"DROP TABLE IF EXISTS raw.{etl.cfg.db_table}")
    
        import pandas as pd 
        df = pd.read_csv(etl.cfg.bronze_filepath,sep=';')
        pg.send_df_to_db(df, table_name=etl.cfg.db_table, filename=etl.cfg.bronze_filepath.name)

    @task
    def t_check_staging_count():
        result = pg.fetchone(f"SELECT COUNT(*) FROM raw.{etl.cfg.db_table}")
        n = result[0] if result else 0
        if n == 0:
            raise ValueError("Staging está vazia, abortando promoção para raw")
        logger_g.info(f"✅ Staging tem {n} linhas")

    @task
    def t_insert():
        pg.execute_query(f"""
            CREATE TABLE IF NOT EXISTS raw.{target} 
            AS SELECT * FROM raw.{etl.cfg.db_table} LIMIT 0;    
            
            TRUNCATE TABLE raw.{target};
            INSERT INTO raw.{target}
            SELECT * FROM raw.{etl.cfg.db_table};
        """)

    @task
    def t_drop_stg_if_exists():
        pg.execute_query(f"DROP TABLE IF EXISTS {etl.cfg.db_table};")

    # ---------- Encadeamento ----------
    extract = t_extract()
    transform = t_transform()
    # validate  = t_validate()
    create_raw = t_create_schema()    
    load_staging = t_load_staging()
    check_staging = t_check_staging_count()
    insert_into_target = t_insert()
    drop_staging = t_drop_stg_if_exists()
    
    # extract >> transform >> validate >> create_raw >> load_staging >> check_staging >> insert_into_target >> drop_staging
    extract >> transform >> create_raw >> load_staging >> check_staging >> insert_into_target >> drop_staging

# Necessário para o Airflow reconhecer a DAG
dag = governismo_pipeline()