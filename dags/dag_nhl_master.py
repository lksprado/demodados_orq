from airflow.decorators import dag
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from pendulum import datetime
import os

from cosmos import (
    DbtTaskGroup,
    ProjectConfig,
    ProfileConfig,
    ExecutionConfig,
    RenderConfig,
)
from cosmos.profiles import PostgresUserPasswordProfileMapping

# -----------------------------
# DEFAULT ARGS
# -----------------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 5),
    "retries": 2,
}

# -----------------------------
# DAG MASTER
# -----------------------------
@dag(
    dag_id="nhl_master_pipeline",
    description="Pipeline master NHL: ingestão -> dbt -> ingestões complementares -> dbt",
    schedule="00 05 * * *",
    catchup=False,
    default_args=default_args,
    tags=["nhl", "master", "orchestration"],
)
def nhl_master_pipeline():

    # -----------------------------
    # DBT CONFIG (COMPARTILHADA)
    # -----------------------------
    profile_config = ProfileConfig(
        profile_name="my_datawarehouse",
        target_name="dev",
        profile_mapping=PostgresUserPasswordProfileMapping(
            conn_id="postgres_dw",
            profile_args={
                "schema": "staging"
            },
        ),
    )
        
    execution_config = ExecutionConfig(
        dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt",
    )

    project_config = ProjectConfig(
        dbt_project_path="/usr/local/airflow/dbt/my_datawarehouse",
    )

    # -----------------------------
    # 1️⃣ INGESTÃO CORE
    # -----------------------------
    trigger_games_summary = TriggerDagRunOperator(
        task_id="trigger_games_summary",
        trigger_dag_id="nhl_games_summary",
        wait_for_completion=True,
        reset_dag_run=True,
    )

    # -----------------------------
    # 2️⃣ DBT RUN – APÓS CORE
    # -----------------------------
    dbt_run_after_summary = DbtTaskGroup(
        group_id="dbt_run_after_games_summary",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(
            select=["tag:nhl", "tag:staging", "tag:game_id"],
        ),
        operator_args={
            "install_deps": True,
            "full_refresh": False,
        },
    )

    # -----------------------------
    # 3️⃣ INGESTÕES COMPLEMENTARES
    # -----------------------------
    trigger_games_summary_details = TriggerDagRunOperator(
        task_id="trigger_games_summary_details",
        trigger_dag_id="nhl_games_summary_details",
        wait_for_completion=True,
        reset_dag_run=True,
    )

    trigger_games_details = TriggerDagRunOperator(
        task_id="trigger_games_details",
        trigger_dag_id="nhl_games_details",
        wait_for_completion=True,
        reset_dag_run=True,
    )

    trigger_games_play_by_play = TriggerDagRunOperator(
        task_id="trigger_games_play_by_play",
        trigger_dag_id="nhl_games_play_by_play",
        wait_for_completion=True,
        reset_dag_run=True,
    )

    trigger_game_log = TriggerDagRunOperator(
        task_id="trigger_game_log",
        trigger_dag_id="nhl_game_log",
        wait_for_completion=True,
        reset_dag_run=True,
    )

    trigger_club_stats = TriggerDagRunOperator(
        task_id="trigger_club_stats",
        trigger_dag_id="nhl_club_stats",
        wait_for_completion=True,
        reset_dag_run=True,
    )

    trigger_players = TriggerDagRunOperator(
        task_id="trigger_players",
        trigger_dag_id="nhl_all_players",
        wait_for_completion=True,
        reset_dag_run=True,
    )

    # -----------------------------
    # 4️⃣ DBT RUN FINAL
    # -----------------------------
    dbt_run_final = DbtTaskGroup(
        group_id="dbt_run_final",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(
            select=["tag:nhl"],
            exclude=["tag:game_id"],
        ),
        operator_args={
            "install_deps": False,
            "full_refresh": False,
        },
    )

    # -----------------------------
    # FLUXO
    # -----------------------------
    trigger_games_summary >> dbt_run_after_summary

    dbt_run_after_summary >> [
        trigger_games_summary_details,
        trigger_games_details,
        trigger_games_play_by_play,
        trigger_game_log,
        trigger_club_stats,
    ]

    [
        trigger_games_summary_details,
        trigger_games_details,
        trigger_games_play_by_play,
        trigger_game_log,
        trigger_club_stats,
        trigger_players
    ] >> dbt_run_final


# -----------------------------
# INSTANTIAÇÃO
# -----------------------------
dag = nhl_master_pipeline()
