FROM apache/airflow:2.10.0-python3.8
USER root

RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    openjdk-17-jdk \
    && apt-get clean

USER airflow

RUN pip install --no-cache-dir \
    dlt[parquet] \
    pandas \
    sqlalchemy \
    pymysql \
    psycopg2-binary \
    pyarrow \
    pyspark==3.5.1