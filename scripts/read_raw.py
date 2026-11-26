"""Читает файл из SeaweedFS через DuckDB и печатает первые строки."""

import argparse
import os

import duckdb


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", required=True, help="s3://raw/... путь к файлу")
    args = parser.parse_args()

    endpoint = os.environ.get("S3_ENDPOINT", "localhost:8333")
    access_key = os.environ["S3_ACCESS_KEY"]
    secret_key = os.environ["S3_SECRET_KEY"]

    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"""
        CREATE OR REPLACE SECRET seaweedfs (
            TYPE s3,
            KEY_ID '{access_key}',
            SECRET '{secret_key}',
            ENDPOINT '{endpoint}',
            URL_STYLE 'path',
            USE_SSL false,
            REGION 'us-east-1'
        );
    """)

    print(f"Читаем {args.path}")
    result = con.execute(
        f"SELECT * FROM read_csv_auto('{args.path}') LIMIT 5"
    ).fetchall()
    for row in result:
        print(row)


if __name__ == "__main__":
    main()