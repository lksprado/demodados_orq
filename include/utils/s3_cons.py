from pathlib import Path
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

import logging 
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

def upload_all_files_to_s3(input_folder, bucket_name, con_id, prefix='bronze/', sufix=".json"):
    """_summary_

    Args:
        input_folder (_type_): Pasta de origem local
        bucket_name (_type_): nome do bucket
        con_id (_type_): nome da conexao airflow
        prefix (str, optional): Caminho até o bucket. Padrão é 'bronze/'
        sufix (str, optional)> Extensao do arquivo. Padrão é '.json'
    """
    folder = Path(input_folder)

    hook = S3Hook(aws_conn_id=con_id)


    files = [p for p in folder.iterdir() if p.is_file() and p.suffix == sufix]
    qt = len(files)
    logger.info(f"{qt} arquivos para enviar")

    for p in files:
            key = f"{prefix}{p.name}"
            hook.load_file(
                filename=str(p),
                key=key,
                bucket_name=bucket_name,
                replace=True
            )
    logger.info(f"{qt} arquivos enviados")