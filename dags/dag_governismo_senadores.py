import logging
from datetime import datetime

from airflow.decorators import dag, task

from src.utils.pipeline_cfg import PipelineConfig, GenericETL
from src.pipelines.legislativo.radar_governismo import transform_governismo
from src.pipelines.legislativo.schema import GovernismoSchema
from src.utils.loaders.postgres import PostgreSQLManager
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger("DAG: governismo")


PIPELINE_GOVERNISMO_SENADORES_CONFIG_PRD = {
    "url_base": "https://radar.congressoemfoco.com.br/api/governismo?casa=senado",
    "landing_dir": "/usr/local/airflow/mylake/raw/demodados/radar_congresso/governismo/",
    "landing_file": "radar_governismo_senadores.json",
    "bronze_dir": "/usr/local/airflow/mylake/bronze/demodados/radar_congresso/governismo/",
    "bronze_file": "radar_governismo_senadores.csv",
    "db_table": "stg_radar_governismo_senadores",
}

@dag(
    dag_id="governismo_senadores_pipeline",
    start_date=datetime(2025, 10, 24),
    schedule="@weekly",
    catchup=False,
    tags=["radar_congresso"],
)
def governismo_pipeline():
    target = 'raw_radar_governismo_senadores'
    
    # Hook/engine do Postgres para dentro da DAG
    hook = PostgresHook(postgres_conn_id="demodadosdw")
    engine = hook.get_sqlalchemy_engine()
    pg = PostgreSQLManager(engine=engine)  # usa engine do Airflow

    # Instanciações
    cfg = PipelineConfig(**PIPELINE_GOVERNISMO_SENADORES_CONFIG_PRD)
    etl = GenericETL(
        cfg=cfg,
        extract_fn=None,                # usa extração genérica (GET único)
        load_fn=None,   # nosso adapter que lê bronze e carrega
        validator=GovernismoSchema,
        log=logger,
    )

    # ------- Tasks (cada uma usa somente cfg/etl) -------
    @task
    def t_extract():
        etl.extract()           # escreve cfg.landing_filepath

    @task
    def t_transform():
        transform_governismo(cfg)   # escreve cfg.bronze_filepath

    @task
    def t_validate():
        etl.validate()          # valida cfg.bronze_filepath

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
        logger.info(f"✅ Staging tem {n} linhas")

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
        pg.execute_query(f"DROP TABLE IF EXISTS raw.{etl.cfg.db_table};")

    # ---------- Encadeamento ----------
    extract = t_extract()
    transform = t_transform()
    validate  = t_validate()
    load_staging = t_load_staging()
    check_staging = t_check_staging_count()
    insert_into_target = t_insert()
    drop_staging = t_drop_stg_if_exists()
    
    extract >> transform >> validate >> load_staging >> check_staging >> insert_into_target >> drop_staging
    
# Necessário para o Airflow reconhecer a DAG
dag = governismo_pipeline()