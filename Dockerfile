FROM astrocrpublic.azurecr.io/runtime:3.0-10


RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-postgres==1.9.0 && deactivate