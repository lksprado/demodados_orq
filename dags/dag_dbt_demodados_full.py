from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
import os

profile_config = ProfileConfig(
    profile_name="demodados",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="demodadosdw",
        profile_args={"schema": "raw"},
    ),
)

# dbt_env = Variable.get("dbt_env", default_var="dev").lower()
# if dbt_env not in ("dev", "prod"):
#     raise ValueError(f"dbt_env inválido: {dbt_env!r}, use 'dev' ou 'prod'")

my_cosmos_dag = DbtDag(
    project_config=ProjectConfig(
        dbt_project_path="/usr/local/airflow/dbt/demodadosdw", ### caminho dentro da maquina docker
        project_name="demodadosdw",
    ),
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt",
    ),
    operator_args={
        "install_deps": True,
        "target": profile_config.target_name,
    },
    schedule="@weekly",
    # start_date=datetime(2025, 10, 25, 21, 5),
    catchup=False,
    dag_id=f"dag_demodados_dbt",
    default_args={"retries": 2},
)