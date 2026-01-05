from airflow.decorators import dag, task
from pendulum import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook

# -----------------------------
# Imports do projeto
# -----------------------------
from include.nhl_extraction.endpoints import get_all_club_stats_endpoint
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
    dag_id="nhl_club_stats",
    default_args=default_args,
    description="ETL for NHL Data with dbt",
    schedule="00 06 * * *",
    catchup=False,
)
def nhl_club_stats():
    config = get_all_club_stats_endpoint()
    
    db_hook = PostgresHook(postgres_conn_id="postgres_dw")
    
    @task
    def extraction():
        """Extrai dados da API e salva em JSON"""
        table_name = 'vw_stg_request_teams_seasons_gametypes_id'
        
        extractor = Extractor()
        
        rows = get_data_from_db(connection_provider=lambda: db_hook.get_conn(), table=table_name, cols=['team_id', 'season_id','game_type_id'], return_as='tuples')
        
        total = len(rows)
        
        for i, (team_id, season_id, game_type_id) in enumerate(rows, start=1):
            
            print(f"Extração: {i} de {total}")

            url = config.url.format(
                team_id=team_id,
                season_id=season_id,
                game_type_id=game_type_id
            )
            
            data = extractor.make_request(url=url)

            filename = config.filename.format(
                team_id=team_id,
                season_id=season_id,
                game_type_id=game_type_id
            )

            extractor.save_json(
                data=data,
                output_dir=config.output_dir,
                filename=filename
            )

    
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
dag = nhl_club_stats()