import logging
from datetime import datetime

import pandas as pd
from airflow.decorators import dag, task

from src.utils.pipeline_cfg import PipelineConfig, GenericETL
from src.pipelines.legislativo.ranking_parlamentares import transform_parlamentares
from src.pipelines.legislativo.schema import ParlamentarRankingSchema
from src.utils.loaders.postgres import PostgreSQLManager
from airflow.providers.postgres.hooks.postgres import PostgresHook


logger = logging.getLogger("DAG: ranking_parlamentares")


PIPELINE_PARLAMENTARES_CONFIG_PRD = {
    "url_base": "https://apirest2.politicos.org.br/api/parliamentarianranking?Include=Parliamentarian.State&Include=Parliamentarian.Party&Include=Parliamentarian.Organ&Include=Parliamentarian&take=700&StatusId=1&OrderBy=scoreRanking&Year=2025",
    "landing_dir": "/usr/local/airflow/mylake/raw/demodados/ranking/parlamentares/",
    "landing_file": "ranking_parlamentares.json",
    "bronze_dir": "/usr/local/airflow/mylake/bronze/demodados/ranking/parlamentares/",
    "bronze_file": "ranking_parlamentares.csv",
    "db_table": "stg_ranking_parlamentares",
}


@dag(
    dag_id="ranking_parlamentares_pipeline",
    start_date=datetime(2025, 10, 24),
    schedule="@weekly",
    catchup=False,
    tags=["ranking_politicos"],
)
def parlamentares_pipeline():
    target = 'raw_ranking_parlamentares'
    
    hook = PostgresHook(postgres_conn_id="demodadosdw")
    engine = hook.get_sqlalchemy_engine()
    pg = PostgreSQLManager(engine=engine)  # usa engine externa
    # Instancia o ETL genérico
    cfg = PipelineConfig(**PIPELINE_PARLAMENTARES_CONFIG_PRD)
    etl = GenericETL(
        cfg=cfg,
        extract_fn=None,
        load_fn=pg,
        validator=ParlamentarRankingSchema,
        log=logger,
    )

    @task
    def t_extract():
        etl.extract()

    @task
    def t_transform():
        transform_parlamentares(cfg)

    @task
    def t_validate():
        etl.validate()

    @task
    def t_load_staging():
        pg.execute_query(f"DROP TABLE IF EXISTS raw.{etl.cfg.db_table}")
    
        import pandas as pd 
        df = pd.read_csv(etl.cfg.bronze_filepath,sep=';')
        pg.send_df_to_db(df, table_name=etl.cfg.db_table, filename=etl.cfg.bronze_filepath.name)
        
        
    @task
    def t_check_staging_count():
        result = pg.fetchone(f"SELECT COUNT(*) FROM raw.{etl.cfg.db_table}")
        if not result or result[0] == 0:
            raise ValueError("Staging está vazia, abortando promoção para raw")
        logger.info(f"✅ Staging tem {result[0]} linhas")

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