from airflow.decorators import dag, task
from pendulum import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook

# -----------------------------
# Imports do projeto
# -----------------------------
from include.nhl_extraction.endpoints import get_all_games_summary_details_endpoint
from include.nhl_extraction.src.extraction.extraction import Extractor
from include.nhl_extraction.src.extraction.controller import get_data_from_db
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
    dag_id="nhl_games_summary_details",
    default_args=default_args,
    description="ETL for NHL Data with dbt",
    schedule=None,
    catchup=False,
    tags=["nhl"]
)
def nhl_games_summary_details():
    config = get_all_games_summary_details_endpoint()
    
    db_hook = PostgresHook(postgres_conn_id="postgres_dw")
    
    @task
    def extraction():
        """Extrai dados da API e salva em JSON"""
        table_name = 'vw_stg_request_games_id'
        
        games = get_data_from_db(connection_provider=lambda: db_hook.get_conn(), table=table_name, cols=['game_id'], bool_filter=('has_games_summary_details', False))
        
        total = len(games)
        games_num = 1
        
        for game in games:
            print(f"Extração: {games_num} de {total}")
            extractor = Extractor()
            url = config.url.format(game_id = game)
            data = extractor.make_request(url)
            extractor.save_json(data=data, output_dir=config.output_dir, filename=config.filename.format(game_id=game))
            games_num +=1
    
    @task
    def loading():
        """Carrega dados JSON na camada raw do banco"""
        loader = Loader(connection_provider=lambda: db_hook.get_conn())
        
        files = list(config.output_dir.glob(config.file_pattern))
        
        loader.load_files(config, files)
    

    # -----------------------------
    # FLUXO
    # -----------------------------
    extract = extraction()
    load = loading()
  
    extract >> load
# Instancia a DAG
dag = nhl_games_summary_details()