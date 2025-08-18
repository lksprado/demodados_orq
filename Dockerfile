FROM apache/airflow:3.0.4

USER root

# Atualizar e instalar dependências
RUN mkdir -p /var/lib/apt/lists/partial && \
    apt-get update && \
    apt-get install -y python3-pip && \
    apt-get install -y vim && \
    apt-get install -y \
    libglib2.0-0 \
    libnss3 \
    libnspr4 \
    libdbus-1-3 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libatspi2.0-0 \
    libxcomposite1 \
    libxdamage1 \
    libxext6 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libdrm2 \
    libxkbcommon0 \
    libasound2 && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow

# Copiar o arquivo requirements.txt e instalar as dependências
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Instalar o Playwright e os navegadores
RUN pip install playwright && playwright install

#ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow/dags"
