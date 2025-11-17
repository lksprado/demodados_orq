FROM astrocrpublic.azurecr.io/runtime:3.0-10


RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
<<<<<<< HEAD
    pip install --no-cache-dir dbt-postgres==1.9.0 && deactivate

# Inclui o projeto de ingestão (submódulo) dentro da imagem para os DAGs importarem
COPY ./include/ingestor /usr/local/airflow/include/ingestor

# Instala dependências específicas do ingestor, se houver
RUN if [ -f /usr/local/airflow/include/ingestor/requirements.txt ]; then \
      pip install --no-cache-dir -r /usr/local/airflow/include/ingestor/requirements.txt; \
    fi

# Instala dependências do projeto Airflow
COPY ./requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
=======
    pip install --no-cache-dir -r ./requirements.txt && deactivate
>>>>>>> voltar-um
