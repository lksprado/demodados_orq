import logging
from datetime import datetime
import os

import pandas as pd
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook


@dag(
    dag_id="extract_postgres_marts",
    start_date=datetime(2025, 11, 17),
    schedule="55 2 * * *",
    catchup=False,
    tags=["gold"],
)
def extract_pipeline():
    logger = logging.getLogger("DAG: postgres")

    GOLD_DIR = "/usr/local/airflow/mylake/gold/"

    # Conexão com DW
    hook = PostgresHook(postgres_conn_id="postgres_dw")
    engine = hook.get_sqlalchemy_engine()

    def _dump_table(schema: str, table: str):
        """
        Lê uma tabela do Postgres e salva em CSV no GOLD_DIR.
        Retorna o caminho do arquivo salvo (útil pra debug/log).
        """
        full_name = f"{schema}.{table}"
        logger.info(f"📤 Exportando {full_name} ...")

        # 1. Carrega tudo da tabela
        df = pd.read_sql(f"SELECT * FROM {full_name}", con=engine)

        # 2. Monta nome do arquivo com timestamp pra versionar
        # ts = pendulum.now("America/Sao_Paulo").strftime("%Y%m%d_%H%M%S")
        # filename = f"{table}_{ts}.csv"
        filename = f"{table}.csv"
        filepath = os.path.join(GOLD_DIR, filename)

        # 3. Salva CSV
        df.to_csv(filepath, sep=';', index=False)

        logger.info(f"✅ Export concluído: {filepath}")
        return filepath

    #
    # Agora criamos UMA task por tabela que você quer exportar.
    # Você pode ir adicionando/removendo tasks depois, bem fácil.
    #

    @task(task_id="export_energia_e_clima")
    def export_export_energia_e_clima():
        return _dump_table(schema="marts", table="mrt_energia_e_clima")

    @task(task_id="export_energia_hora")
    def export_energia_hora():
        return _dump_table(schema="marts", table="mrt_energia_hora")


    # Se você quiser mais tabelas, cria mais @task copiando esse padrão.

    # IMPORTANTE:
    # Aqui a gente simplesmente INSTANCIA as tasks.
    # Não vamos encadear com >> porque você pediu que elas sejam independentes.
    t1 = export_export_energia_e_clima()
    t2 = export_energia_hora()

    # Nenhuma dependência entre dep, vot, par.
    # Isso significa:
    # - Airflow pode rodar todas em paralelo quando você dá trigger.
    # - Se uma falhar, as outras rodam normal.
    # - Você pode até só marcar "run" em uma task específica pela UI se quiser gerar só uma tabela.

    # opcional: você pode retornar algo aqui só pra não ficar "variável não usada"
    return [t1, t2]


dag = extract_pipeline()
