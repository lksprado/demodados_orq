from airflow.decorators import dag, task
from pendulum import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook

# -----------------------------
# Imports do projeto
# -----------------------------
from include.nhl_extraction.endpoints import get_all_players_gamelog_endpoint
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
    dag_id="nhl_game_log",
    default_args=default_args,
    description="ETL for NHL Data with dbt",
    schedule="00 06 * * *",
    catchup=False,
)
def nhl_game_log():
    config = get_all_players_gamelog_endpoint()
    
    db_hook = PostgresHook(postgres_conn_id="postgres_dw")
    
    @task
    def extraction():
        """Extrai dados da API e salva em JSON"""
        table_name = 'vw_stg_request_players_seasons_gametypes_id'
        
        extractor = Extractor()
        
        rows = get_data_from_db(connection_provider=lambda: db_hook.get_conn(), table=table_name, cols=['player_id', 'season_id','game_type_id'], return_as='tuples')
        
        total = len(rows)
        
        for i, (player_id, season_id, game_type_id) in enumerate(rows, start=1):
            
            print(f"Extração: {i} de {total}")

            url = config.url.format(
                player_id=player_id,
                season_id=season_id,
                game_type_id=game_type_id
            )
            
            data = extractor.make_request(url=url)

            filename = config.filename.format(
                player_id=player_id,
                season_id=season_id,
                game_type_id=game_type_id
            )

            output_dir = config.output_dir / str(season_id)
            
            extractor.save_json(
                data=data,
                output_dir=output_dir,
                filename=filename
            )

    
    @task
    def loading():
        """Carrega dados JSON na camada raw do banco"""
        loader = Loader(connection_provider=lambda: db_hook.get_conn())

        base_dir = config.output_dir

        subdirs = [d for d in base_dir.iterdir() if d.is_dir()]
        if not subdirs:
            loader.logger.warning(f"No season folders found under {base_dir}")
            return
        latest = max(subdirs, key=lambda p: p.name)
        season_dirs = [latest]

        files = config.collect_files(season_dirs)
        
        loader.load_files(config, files)
    

    # -----------------------------
    # FLUXO
    # -----------------------------
    extract = extraction()
    load = loading()
  
    extract >> load
# Instancia a DAG
dag = nhl_game_log()