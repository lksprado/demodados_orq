from __future__ import annotations

import os
import sys
from typing import Dict

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from pendulum import datetime


AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/usr/local/airflow")
# Garante que os imports do projeto ingestor funcionem (usa pacote "src/")
INGESTOR_ROOT = f"{AIRFLOW_HOME}/include/ingestor"
if INGESTOR_ROOT not in sys.path:
    sys.path.append(INGESTOR_ROOT)


def export_db_env_from_conn(conn_id: str = "demodadosdw") -> Dict[str, str]:
    """Exporta variáveis de ambiente esperadas pelo loader do local_setup a partir da Connection do Airflow.

    local_setup/src/utils/loaders/postgres.py espera: DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT
    """
    hook = PostgresHook(postgres_conn_id=conn_id)
    conn = hook.get_connection(hook.postgres_conn_id)
    env = {
        "DB_NAME": conn.schema or "postgres",
        "DB_USER": conn.login,
        "DB_PASSWORD": conn.password or "",
        "DB_HOST": conn.host or "localhost",
        "DB_PORT": str(conn.port or 5432),
    }
    os.environ.update(env)
    return env


def make_cfg(
    landing_dir: str,
    landing_file: str,
    bronze_dir: str,
    bronze_file: str,
    db_table: str,
    url_base: str,
):
    """Helper para construir o dict de configuração esperado pelo PipelineConfig."""
    from pathlib import Path

    return dict(
        url_base=url_base,
        parameter_file=None,
        landing_dir=str(Path(landing_dir)),
        landing_file=landing_file,
        bronze_dir=str(Path(bronze_dir)),
        bronze_file=bronze_file,
        db_table=db_table,
    )


@dag(schedule="@daily", start_date=datetime(2025, 9, 1), catchup=False, tags=["ingest", "legislativo"])
def ingest_legislativo():
    dbt_env = Variable.get("dbt_env", default_var="dev").lower()
    lake_base = Variable.get("lake_base_dir", default_var=f"{AIRFLOW_HOME}/mylake")

    # Pastas no volume persistente montado (raw/bronze)
    base_raw = f"{lake_base}/raw/radar_congresso"
    base_bronze = f"{lake_base}/bronze/radar_congresso"

    # Helpers que reaproveitam o código do local_setup
    def _extract(cfg_dict):
        export_db_env_from_conn("demodadosdw")
        from src.utils.pipeline_cfg import PipelineConfig, GenericETL

        etl = GenericETL(cfg=PipelineConfig(**cfg_dict))
        etl.extract()  # generic_extraction usa url_base/landing_file

    def _transform_governismo(cfg_dict):
        from src.utils.pipeline_cfg import PipelineConfig
        from src.pipelines.legislativo.radar_congresso import transform_governismo

        cfg = PipelineConfig(**cfg_dict)
        transform_governismo(None, cfg)  # lê landing e escreve bronze

    def _validate_governismo(cfg_dict):
        import pandas as pd
        from src.utils.pipeline_cfg import PipelineConfig
        from src.pipelines.legislativo.schema import GovernismoSchema

        cfg = PipelineConfig(**cfg_dict)
        df = pd.read_csv(cfg.bronze_filepath, sep=";")
        GovernismoSchema.validate(df)

    def _load(cfg_dict):
        import pandas as pd
        from src.utils.pipeline_cfg import PipelineConfig
        from src.utils.loaders.postgres import PostgreSQLManager

        export_db_env_from_conn("demodadosdw")
        cfg = PipelineConfig(**cfg_dict)
        df = pd.read_csv(cfg.bronze_filepath, sep=";")
        pg = PostgreSQLManager()
        # Emula GenericETL.generic_loader (truncate + append)
        pg.truncate_table(table_name=cfg.db_table, log=None)
        pg.send_df_to_db(df=df, table_name=cfg.db_table, filename=cfg.bronze_file, how="append", log=None)

    # Configs específicas
    cfg_dep = make_cfg(
        landing_dir=f"{base_raw}/governismo",
        landing_file="radar_governismo_deputados.json",
        bronze_dir=f"{base_bronze}/governismo",
        bronze_file="radar_governismo_deputados.csv",
        db_table="radar_governismo_deputados_raw",
        url_base="https://radar.congressoemfoco.com.br/api/governismo?casa=camara",
    )

    cfg_sen = make_cfg(
        landing_dir=f"{base_raw}/governismo",
        landing_file="radar_governismo_senadores.json",
        bronze_dir=f"{base_bronze}/governismo",
        bronze_file="radar_governismo_senadores.csv",
        db_table="radar_governismo_senadores_raw",
        url_base="https://radar.congressoemfoco.com.br/api/governismo?casa=senado",
    )

    @task(retries=2)
    def dep_extract():
        _extract(cfg_dep)

    @task(retries=2)
    def dep_transform():
        _transform_governismo(cfg_dep)

    @task(retries=2)
    def dep_validate():
        _validate_governismo(cfg_dep)

    @task(retries=2)
    def dep_load():
        _load(cfg_dep)

    @task(retries=2)
    def sen_extract():
        _extract(cfg_sen)

    @task(retries=2)
    def sen_transform():
        _transform_governismo(cfg_sen)

    @task(retries=2)
    def sen_validate():
        _validate_governismo(cfg_sen)

    @task(retries=2)
    def sen_load():
        _load(cfg_sen)

    trigger_dbt = TriggerDagRunOperator(
        task_id="run_dbt_after_ingest",
        trigger_dag_id=f"dag_demodados_dw_{dbt_env}",
        wait_for_completion=True,
    )

    dep_chain = dep_extract() >> dep_transform() >> dep_validate() >> dep_load()
    sen_chain = sen_extract() >> sen_transform() >> sen_validate() >> sen_load()
    [dep_chain, sen_chain] >> trigger_dbt


dag = ingest_legislativo()
