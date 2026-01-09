import logging
from datetime import datetime

from airflow.decorators import dag, task

from include.local_setup.src.utils.pipeline_cfg import PipelineConfig, GenericETL
from include.local_setup.src.pipelines.legislativo.parlamento_senadores import transform_senadores
from include.local_setup.src.pipelines.legislativo.schema import SenadorSchema
from include.local_setup.src.utils.loaders.postgres import PostgreSQLManager
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger("DAG: governismo")


PIPELINE_SENADORES_CONFIG_PRD = {
    "url_base": "https://legis.senado.leg.br/dadosabertos/senador/lista/atual?v=4",
    "landing_dir": "/usr/local/airflow/mylake/raw/demodados/senado/senadores/",
    "landing_file": "senado_senadores.json",
    "bronze_dir": "/usr/local/airflow/mylake/bronze/demodados/senado/senadores/",
    "bronze_file": "senadores_senadores.csv",
    "db_table": "stg_parlamento_senadores_raw",
}

@dag(
    dag_id="senadores_pipeline",
    start_date=datetime(2025, 9, 25),
    # schedule="@weekly",
    schedule=None,
    catchup=False,
    tags=["demodados"],
)
def senadores_pipeline():
    target = 'raw_parlamento_senadores' 
    
    hook = PostgresHook(postgres_conn_id="demodadosdw")
    engine = hook.get_sqlalchemy_engine()
    pg = PostgreSQLManager(engine=engine)  # usa engine externa
    # Instancia o ETL genérico
    cfg = PipelineConfig(**PIPELINE_SENADORES_CONFIG_PRD)
    etl = GenericETL(
        cfg=cfg,
        extract_fn=None,
        load_fn=pg,
        validator=SenadorSchema,
        log=logger,
    )

    @task
    def t_extract():
        etl.extract()

    @task
    def t_transform():
        transform_senadores(cfg)

    @task
    def t_validate():
        etl.validate()

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
    create_raw = t_create_schema()
    load_staging = t_load_staging()
    check_staging = t_check_staging_count()
    insert_into_target = t_insert()
    drop_staging = t_drop_stg_if_exists()
    
    extract >> transform >> validate >> create_raw >> load_staging >> check_staging >> insert_into_target >> drop_staging

# necessário para o Airflow reconhecer a DAG
dag = senadores_pipeline()