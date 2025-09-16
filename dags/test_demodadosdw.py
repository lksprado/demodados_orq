# ~/airflow3/dags/teste_conexao_demodadosdw.py
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

@dag(
    schedule=None,
    start_date=datetime(2025, 8, 11),
    catchup=False,
    tags=["infra", "teste", "db"],
    description="Teste de conexão usando o connectionId 'demodadosdw' (SELECT 1)"
)
def teste_conexao_demodadosdw():

    @task(retries=2, retry_delay=timedelta(seconds=10))
    def testar():
        print("Iniciando teste")
        hook = PostgresHook(postgres_conn_id="demodadosdw")

        print("Fazendo query")
        val = hook.get_first("SELECT 1")[0]
        print(f"resposta query {val}")
        assert val == 1, "Falha ao executar SELECT 1"

        # Log seguro a partir do Airflow Connection (não do psycopg2)
        airflow_conn = hook.get_connection(hook.postgres_conn_id)
        print(
            f"✅ Conexão OK: host={airflow_conn.host} "
            f"db={airflow_conn.schema} user={airflow_conn.login} port={airflow_conn.port}"
        )

    testar()

dag = teste_conexao_demodadosdw()
