FROM apache/airflow:2.9.0-python3.11

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    ca-certificates \
    openssl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir --upgrade \
    urllib3==2.0.7 \
    requests \
    beautifulsoup4 \
    lxml \
    clickhouse-driver