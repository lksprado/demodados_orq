from airflow.models import Variable
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
from airflow.sensors.external_task import ExternalTaskSensor
import os
from pendulum import datetime

profile_config_dev = ProfileConfig(
    profile_name="datawarehouse",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="postgres_dw",
        profile_args={"schema": "raw"},
    ),
)

dbt_env = Variable.get("dbt_env", default_var="dev").lower()


my_cosmos_dag = DbtDag(
    project_config=ProjectConfig(
        dbt_project_path="/usr/local/airflow/dbt/the_dw", ### caminho dentro da maquina docker
        project_name="the_dw",
    ),
    profile_config=profile_config_dev,
    execution_config=ExecutionConfig(
        dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt",
    ),
    operator_args={
        "install_deps": True,
        "target": profile_config_dev.target_name,
    },
    schedule="@weekly",
    # start_date=datetime(2025, 10, 25, 21, 5),
    catchup=False,
    dag_id=f"dag_the_dw_{dbt_env}_full",
    default_args={"retries": 2},
)