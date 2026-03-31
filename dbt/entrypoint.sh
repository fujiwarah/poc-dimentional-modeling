#!/bin/sh
# dbt 実行エントリポイント。
# BigQuery Emulator 接続時は dbt_wrapper.py 経由で monkey patch を適用する。
set -e
exec "$@"
