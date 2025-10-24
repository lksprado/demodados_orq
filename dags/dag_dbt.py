from airflow.models import Variable
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
from airflow.sensors.external_task import ExternalTaskSensor
import os
from pendulum import datetime

profile_config_dev = ProfileConfig(
    profile_name="demodados",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="demodadosdw",
        profile_args={"schema": "raw"},
    ),
)

dbt_env = Variable.get("dbt_env", default_var="dev").lower()
if dbt_env not in ("dev", "prod"):
    raise ValueError(f"dbt_env inválido: {dbt_env!r}, use 'dev' ou 'prod'")

profile_config = profile_config_dev 

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
    start_date=datetime(2025, 10, 25, 21, 5),
    catchup=False,
    dag_id=f"dag_demodados_dw_{dbt_env}",
    default_args={"retries": 2},
    #atualizando
)

# A partir daqui você vai trabalhar **dentro do contexto da DAG**
with my_cosmos_dag:

    # Criar os sensores
    aguarda_deputados = ExternalTaskSensor(
        task_id="aguarda_governismo_deputados",
        external_dag_id="governismo_deputados_pipeline",
        external_task_id=None,
        allowed_states=["success"],
        failed_states=["failed"],
        mode="poke",
    )

    aguarda_senadores = ExternalTaskSensor(
        task_id="aguarda_governismo_senadores",
        external_dag_id="governismo_senadores_pipeline",
        external_task_id=None,
        allowed_states=["success"],
        failed_states=["failed"],
        mode="poke",
    )

    aguarda_parlamentares = ExternalTaskSensor(
        task_id="aguarda_radar_parlamentares",
        external_dag_id="radar_parlamentares_pipeline",
        external_task_id=None,
        allowed_states=["success"],
        failed_states=["failed"],
        mode="poke",
    )

    # Pegue a primeira task criada automaticamente pelo cosmos
    primeira_task_dbt = list(my_cosmos_dag.tasks)[0]  # isso pega a primeira task do DAG

    # Defina que os sensores são pré-requisitos
    aguarda_deputados >> primeira_task_dbt
    aguarda_senadores >> primeira_task_dbt
    aguarda_parlamentares >> primeira_task_dbt