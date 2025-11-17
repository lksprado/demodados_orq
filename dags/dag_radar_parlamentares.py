import logging
from datetime import datetime

from airflow.decorators import dag, task

from src.utils.pipeline_cfg import PipelineConfig, GenericETL
from src.pipelines.legislativo.radar_parlamentares import transform_parlamentares
from src.pipelines.legislativo.schema import ParlamentarRadarSchema
from src.utils.loaders.postgres import PostgreSQLManager
from airflow.providers.postgres.hooks.postgres import PostgresHook


logger = logging.getLogger("DAG: radar_parlamentares")


PIPELINE_PARLAMENTARES_CONFIG_PRD = {
    "url_base": "https://radar.congressoemfoco.com.br/api/busca-parlamentar",
    "landing_dir": "/usr/local/airflow/mylake/raw/demodados/radar_congresso/parlamentares/",
    "landing_file": "radar_parlamentares.json",
    "bronze_dir": "/usr/local/airflow/mylake/bronze/demodados/radar_congresso/parlamentares/",
    "bronze_file": "radar_parlamentares.csv",
    "db_table": "stg_radar_parlamentares",
}


@dag(
    dag_id="radar_parlamentares_pipeline",
    start_date=datetime(2025, 10, 24),
    schedule="@weekly",
    catchup=False,
    tags=["radar_congresso"],
)
def parlamentares_pipeline():
    target = 'raw_radar_parlamentares'
    
    hook = PostgresHook(postgres_conn_id="demodadosdw")
    engine = hook.get_sqlalchemy_engine()
    pg = PostgreSQLManager(engine=engine)  # usa engine externa

    # # Adapter de load_fn no “estilo cfg”: assina (cfg) -> None
    # def load_staging_via_cfg(cfg: PipelineConfig) -> None:
    #     # Respeite o mesmo sep do transform/validate (aqui ";")
    #     import pandas as pd
    #     df = pd.read_csv(cfg.bronze_filepath, sep=";")
    #     pg.send_df_to_db(df, table_name=cfg.db_table, how="replace")

    # Instanciações
    cfg = PipelineConfig(**PIPELINE_PARLAMENTARES_CONFIG_PRD)
    etl = GenericETL(
        cfg=cfg,
        extract_fn=None,                # usa extração genérica (GET único)
        load_fn=None,   # nosso adapter que lê bronze e carrega
        validator=ParlamentarRadarSchema,
        log=logger,
    )

    # ------- Tasks (cada uma usa somente cfg/etl) -------
    @task
    def t_extract():
        etl.extract()           # escreve cfg.landing_filepath

    @task
    def t_transform():
        transform_parlamentares(cfg)   # escreve cfg.bronze_filepath

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
        pg.execute_query(f"""
            DROP TABLE IF EXISTS raw.{etl.cfg.db_table};
        """)

    # ---------- Encadeamento ----------
    extract = t_extract()
    transform = t_transform()
    validate  = t_validate()
    load_staging = t_load_staging()
    check_staging = t_check_staging_count()
    insert_into_target = t_insert()
    drop_staging = t_drop_stg_if_exists()
    
    extract >> transform >> validate >> load_staging >> check_staging >> insert_into_target >> drop_staging

# 👇 necessário para o Airflow reconhecer a DAG
dag = parlamentares_pipeline()