"""DAG: загрузка исходного файла источника в raw-слой SeaweedFS."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)

# --- Параметры источника (наш OWID Energy Data) ---
SOURCE_PATH = "/owid/energy-data/master/owid-energy-data.csv"
SOURCE_FILENAME = "owid-energy-data.csv"
DATASET_SLUG = "owid_energy"
S3_CONN_ID = "s3_conn"
RAW_BUCKET = "raw"


def load_to_raw(ds: str, **kwargs) -> None:
    """Скачивает файл по HTTP и кладёт его в raw-бакет без изменений."""
    key = f"{DATASET_SLUG}/ingested_on={ds}/{SOURCE_FILENAME}"

    s3 = S3Hook(aws_conn_id=S3_CONN_ID)

    if not s3.check_for_bucket(RAW_BUCKET):
        raise RuntimeError(
            f"Бакет '{RAW_BUCKET}' не найден. "
            "Выполните 'docker compose up -d' и дождитесь старта SeaweedFS."
        )

    if s3.check_for_key(key, bucket_name=RAW_BUCKET):
        log.info("Объект уже существует, пропускаем загрузку: s3://%s/%s", RAW_BUCKET, key)
        return

    conn = BaseHook.get_connection("source_http_conn")
    url = f"{conn.host}{SOURCE_PATH}"

    log.info("Скачиваем %s", url)
    response = requests.get(url, timeout=120)
    response.raise_for_status()

    s3.load_bytes(
        response.content,
        key=key,
        bucket_name=RAW_BUCKET,
        replace=False,
    )
    log.info("Загружено s3://%s/%s (%d байт)", RAW_BUCKET, key, len(response.content))


default_args = {
    "owner": "aakor",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="ingest_raw",
    description="Ingestion исходного файла OWID Energy в raw SeaweedFS",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["raw", "ingestion"],
) as dag:

    wait_for_primary_source = HttpSensor(
        task_id="wait_for_primary_source",
        http_conn_id="source_http_conn",
        endpoint=SOURCE_PATH,
        method="HEAD",
        poke_interval=60,
        timeout=600,
        mode="reschedule",
    )

    load_to_raw_task = PythonOperator(
        task_id="load_to_raw",
        python_callable=load_to_raw,
    )

    wait_for_primary_source >> load_to_raw_task