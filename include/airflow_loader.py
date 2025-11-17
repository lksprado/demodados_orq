import logging
from datetime import datetime
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

class AirflowPostgreSQLManager:
    def __init__(self, conn_id="demodados_dw"):
        self.hook = PostgresHook(postgres_conn_id=conn_id)
        self.engine = self.hook.get_sqlalchemy_engine()

    def send_df_to_db(
        self,
        df: pd.DataFrame,
        table_name: str,
        how="replace",
        filename=None,
        schema="raw",
        log: logging.Logger = None,
    ):
        logger = log or logging.getLogger("AirflowPostgreSQLManager")
        try:
            if filename:
                df["arquivo_origem"] = filename
            df["data_carga"] = datetime.now()

            with self.engine.begin() as conn:  # 🔑 sempre conexão SQLAlchemy
                df.to_sql(
                    name=table_name,
                    con=conn,
                    schema=schema,
                    if_exists=how,
                    index=False,
                )
            logger.info(f"✅ DADOS INSERIDOS EM {schema}.{table_name}")
        except Exception as e:
            logger.error(f"❌ ERRO AO INSERIR NO BANCO: {e}", exc_info=True)
            raise

    def truncate_table(self, table_name: str, schema: str = "raw", log: logging.Logger = None):
        logger = log or logging.getLogger("AirflowPostgreSQLManager")
        try:
            stmt = f'TRUNCATE TABLE "{schema}"."{table_name}"'
            with self.engine.begin() as conn:
                conn.exec_driver_sql(stmt)
            logger.info(f"TABELA TRUNCADA --- {schema}.{table_name}")
        except Exception as e:
            logger.error(f"❌ ERRO AO TRUNCAR TABELA: {e}", exc_info=True)
            raise
