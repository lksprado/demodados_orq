from airflow.decorators import dag, task
from pendulum import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook
import os
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping

# -----------------------------
# Imports do projeto
# -----------------------------
from include.nhl_extraction.endpoints import get_all_seasons_id_endpoint
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
    dag_id="nhl_seasons",
    default_args=default_args,
    description="ETL for NHL Data with dbt",
    schedule="00 00 01 10 *",
    catchup=False,
)
def nhl_seasons():
    config = get_all_seasons_id_endpoint()
    url = config.url
    filename = config.filename
    out_dir = config.output_dir
    
    db_hook = PostgresHook(postgres_conn_id="postgres_dw")
    
    # Configuração do profile do dbt usando Cosmos
    profile_config = ProfileConfig(
        profile_name="my_datawarehouse",
        target_name="dev",
        profile_mapping=PostgresUserPasswordProfileMapping(
            conn_id="postgres_dw",
            profile_args={"schema": "raw"},
        ),
    )
    
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


    # Task Group do dbt para executar TODO o projeto
    dbt_run_all = DbtTaskGroup(
        group_id="dbt_run_all",
        project_config=ProjectConfig(
            dbt_project_path="/usr/local/airflow/dbt/my_datawarehouse",
        ),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt",
        ),
        render_config=RenderConfig(
            # Executa todos os modelos do projeto
            select=["*"],
        ),
        operator_args={
            "install_deps": True,
            "full_refresh": False,  # True se quiser recriar tudo do zero
        },
        default_args={"retries": 1},
    )
    
    # -----------------------------
    # FLUXO
    # -----------------------------
    extract = extraction()
    load = loading()
  
    extract >> load >> dbt_run_all
# Instancia a DAG
dag = nhl_seasons()