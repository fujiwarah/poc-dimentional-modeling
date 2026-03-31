"""EL パイプラインの設定モジュール。

環境変数から PostgreSQL / BigQuery Emulator の接続情報を読み取る。
デフォルト値は docker-compose.yml の定義に合わせている。
"""

import os

# --- PostgreSQL 接続情報 ---
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.environ.get("POSTGRES_PORT", "5432"))
POSTGRES_USER = os.environ.get("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "postgres")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "ecsite")

# --- BigQuery Emulator 接続情報 ---
BIGQUERY_API_ENDPOINT = os.environ.get("BIGQUERY_API_ENDPOINT", "http://bigquery-emulator:9050")
BIGQUERY_PROJECT = os.environ.get("BIGQUERY_PROJECT", "poc-project")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", "raw")

# --- 対象テーブル一覧 ---
TABLES = [
    "customers",
    "categories",
    "products",
    "coupons",
    "orders",
    "order_items",
    "page_views",
    "inventory_snapshots",
]
