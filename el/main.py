"""EL パイプライン: PostgreSQL -> BigQuery Emulator (raw データセット)。

PostgreSQL の全テーブルからデータを抽出し、BigQuery Emulator の raw データセットに
WRITE_TRUNCATE（洗い替え）でロードする。変換は行わない（Raw Data Load）。
"""

import json
import logging
import sys
from datetime import date, datetime
from decimal import Decimal

import psycopg2
import psycopg2.extras
from google.api_core.client_options import ClientOptions
from google.auth.credentials import AnonymousCredentials
from google.cloud import bigquery

import config

# --- ログ設定 ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# --- PostgreSQL -> BigQuery 型マッピング用スキーマ定義 ---
# DDL に基づいて各テーブルのスキーマを明示的に定義する
TABLE_SCHEMAS: dict[str, list[bigquery.SchemaField]] = {
    "customers": [
        bigquery.SchemaField("customer_id", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("first_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("last_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("email", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("phone", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("city", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("prefecture", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("postal_code", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("created_at", "TIMESTAMP", mode="NULLABLE"),
    ],
    "categories": [
        bigquery.SchemaField("category_id", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("parent_id", "INT64", mode="NULLABLE"),
    ],
    "products": [
        bigquery.SchemaField("product_id", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("product_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("category_id", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("unit_price", "NUMERIC", mode="REQUIRED"),
        bigquery.SchemaField("description", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("created_at", "TIMESTAMP", mode="NULLABLE"),
    ],
    "orders": [
        bigquery.SchemaField("order_id", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("customer_id", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("order_date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("shipping_address", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("status", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("created_at", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("updated_at", "TIMESTAMP", mode="NULLABLE"),
    ],
    "order_items": [
        bigquery.SchemaField("order_item_id", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("order_id", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("product_id", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("quantity", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("unit_price", "NUMERIC", mode="REQUIRED"),
        bigquery.SchemaField("discount", "NUMERIC", mode="NULLABLE"),
    ],
}


def get_pg_connection():
    """PostgreSQL に接続して connection を返す。"""
    logger.info(
        "PostgreSQL に接続: host=%s port=%s db=%s user=%s",
        config.POSTGRES_HOST,
        config.POSTGRES_PORT,
        config.POSTGRES_DB,
        config.POSTGRES_USER,
    )
    conn = psycopg2.connect(
        host=config.POSTGRES_HOST,
        port=config.POSTGRES_PORT,
        user=config.POSTGRES_USER,
        password=config.POSTGRES_PASSWORD,
        dbname=config.POSTGRES_DB,
    )
    return conn


def get_bq_client() -> bigquery.Client:
    """BigQuery Emulator に接続するクライアントを返す。"""
    logger.info(
        "BigQuery Emulator に接続: endpoint=%s project=%s",
        config.BIGQUERY_API_ENDPOINT,
        config.BIGQUERY_PROJECT,
    )
    client = bigquery.Client(
        project=config.BIGQUERY_PROJECT,
        client_options=ClientOptions(api_endpoint=config.BIGQUERY_API_ENDPOINT),
        credentials=AnonymousCredentials(),
    )
    return client


def extract_table(conn, table_name: str) -> tuple[list[dict], list[str]]:
    """PostgreSQL からテーブルのデータを全件抽出する。

    Returns:
        (rows, column_names): 辞書のリストとカラム名のリスト
    """
    logger.info("[%s] PostgreSQL からデータを抽出中...", table_name)
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(f"SELECT * FROM {table_name}")  # noqa: S608
        rows = cur.fetchall()
        column_names = [desc[0] for desc in cur.description]
    logger.info("[%s] 抽出件数: %d 件", table_name, len(rows))
    return rows, column_names


def _serialize_value(value):
    """BigQuery JSON ロード用に Python の値をシリアライズ可能な形式に変換する。"""
    if value is None:
        return None
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return value


def load_table(bq_client: bigquery.Client, table_name: str, rows: list[dict]) -> int:
    """BigQuery Emulator の raw データセットにデータをロードする。

    load_table_from_json を使用し、WRITE_TRUNCATE で洗い替えする。
    動作しない場合は insert_rows にフォールバックする。

    Returns:
        ロードされた行数
    """
    table_id = f"{config.BIGQUERY_PROJECT}.{config.BIGQUERY_DATASET}.{table_name}"
    schema = TABLE_SCHEMAS[table_name]

    logger.info("[%s] BigQuery へロード中... (table_id=%s)", table_name, table_id)

    if not rows:
        logger.warning("[%s] ロードするデータがありません", table_name)
        return 0

    # 値をシリアライズ可能な形式に変換
    serialized_rows = []
    for row in rows:
        serialized_row = {k: _serialize_value(v) for k, v in row.items()}
        serialized_rows.append(serialized_row)

    # 方式 1: load_table_from_json (WRITE_TRUNCATE で洗い替え)
    try:
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        )
        load_job = bq_client.load_table_from_json(
            serialized_rows,
            table_id,
            job_config=job_config,
        )
        load_job.result()  # ジョブ完了を待機
        logger.info("[%s] load_table_from_json でロード完了", table_name)
        return len(serialized_rows)
    except Exception as e:
        logger.warning(
            "[%s] load_table_from_json が失敗、insert_rows にフォールバック: %s",
            table_name,
            e,
        )

    # 方式 2: フォールバック --- テーブルを作成して insert_rows で挿入
    try:
        # テーブルを(再)作成して洗い替え
        table_ref = bigquery.Table(table_id, schema=schema)
        bq_client.delete_table(table_id, not_found_ok=True)
        bq_client.create_table(table_ref)
        logger.info("[%s] テーブルを再作成しました", table_name)

        # insert_rows はタプルのリストを受け取る
        column_names = [field.name for field in schema]
        rows_to_insert = []
        for row in serialized_rows:
            rows_to_insert.append({col: row.get(col) for col in column_names})

        errors = bq_client.insert_rows_json(table_id, rows_to_insert)
        if errors:
            raise RuntimeError(f"insert_rows_json でエラー: {errors}")

        logger.info("[%s] insert_rows_json でロード完了", table_name)
        return len(rows_to_insert)
    except Exception as e2:
        logger.error("[%s] フォールバックも失敗: %s", table_name, e2)
        raise


def run_el():
    """全テーブルの EL パイプラインを実行する。"""
    logger.info("=== EL パイプライン開始 ===")

    failed_tables = []
    pg_conn = None
    bq_client = None

    try:
        pg_conn = get_pg_connection()
        bq_client = get_bq_client()

        for table_name in config.TABLES:
            try:
                logger.info("--- [%s] 処理開始 ---", table_name)

                # Extract
                rows, column_names = extract_table(pg_conn, table_name)
                extracted_count = len(rows)

                # Load
                loaded_count = load_table(bq_client, table_name, rows)

                logger.info(
                    "--- [%s] 処理完了: 抽出=%d件, ロード=%d件 ---",
                    table_name,
                    extracted_count,
                    loaded_count,
                )
            except Exception as e:
                logger.error("[%s] 処理失敗: %s", table_name, e)
                failed_tables.append(table_name)

    except Exception as e:
        logger.error("致命的なエラー: %s", e)
        sys.exit(1)
    finally:
        if pg_conn:
            pg_conn.close()
            logger.info("PostgreSQL 接続をクローズしました")

    if failed_tables:
        logger.error("=== EL パイプライン完了（一部失敗）: %s ===", failed_tables)
        sys.exit(1)

    logger.info("=== EL パイプライン正常完了 ===")
    sys.exit(0)


if __name__ == "__main__":
    run_el()
