from airflow.decorators import dag, task
from pendulum import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook


# -----------------------------
# Imports do projeto
# -----------------------------
from include.nhl_extraction.endpoints import get_all_games_summary_endpoint
from include.nhl_extraction.src.extraction.extraction import Extractor
from include.nhl_extraction.src.loading.loader import Loader

# -----------------------------
# DAG CONFIG
# -----------------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 4),
    "retries": 0,
}

@dag(
    dag_id="nhl_games_summary",
    default_args=default_args,
    description="ETL for NHL Data with dbt",
    schedule=None,
    catchup=False,
    tags=["nhl"]
)
def nhl_games_summary():
    config = get_all_games_summary_endpoint()
    url = config.url
    filename = config.filename
    out_dir = config.output_dir
    
    db_hook = PostgresHook(postgres_conn_id="postgres_dw")
    
    @task
    def extraction():
        """Extrai dados da API e salva em JSON"""
        extractor = Extractor()
        data_extracted = extractor.make_request(url)
        extractor.save_json(data=data_extracted, output_dir=out_dir, filename=filename)
    
    @task
    def loading():
        """Carrega dados JSON na camada raw do banco"""
        loader = Loader(connection_provider=lambda: db_hook.get_conn())
        loader.load(config)
        return "Dados carregados na raw"

    
    # -----------------------------
    # FLUXO
    # -----------------------------
    extract = extraction()
    load = loading()
  
    extract >> load
# Instancia a DAG
dag = nhl_games_summary()