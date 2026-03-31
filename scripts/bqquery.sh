#!/usr/bin/env bash
# BigQuery Emulator にクエリを発行し、カラム名付き JSON で結果を表示する
#
# Usage:
#   ./scripts/bqquery.sh "SELECT * FROM dwh.dim_customer LIMIT 5"
#   ./scripts/bqquery.sh queries/analysis.sql          # ファイル渡し（先頭のクエリを実行）
#   ./scripts/bqquery.sh --table                       # テーブル形式で表示
#   ./scripts/bqquery.sh --csv "SELECT ..."            # CSV 形式で表示

set -euo pipefail

ENDPOINT="${BIGQUERY_ENDPOINT:-http://localhost:9050}"
PROJECT="${BIGQUERY_PROJECT:-poc-project}"
FORMAT="json"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --table) FORMAT="table"; shift ;;
    --csv)   FORMAT="csv";   shift ;;
    *)       break ;;
  esac
done

QUERY="${1:?Usage: bqquery.sh [--table|--csv] <SQL or .sql file>}"

# .sql ファイルが渡された場合は中身を読む（セミコロンで区切られた最初のクエリ）
if [[ -f "$QUERY" ]]; then
  QUERY=$(sed '/^--/d' "$QUERY" | tr '\n' ' ' | sed 's/;.*//')
fi

RESPONSE=$(curl -sf -X POST "${ENDPOINT}/bigquery/v2/projects/${PROJECT}/queries" \
  -H "Content-Type: application/json" \
  -d "$(jq -n --arg q "$QUERY" '{query: $q, useLegacySql: false}')")

# エラーチェック
if echo "$RESPONSE" | jq -e '.error' > /dev/null 2>&1; then
  echo "$RESPONSE" | jq -r '.error.message // .error' >&2
  exit 1
fi

TOTAL=$(echo "$RESPONSE" | jq -r '.totalRows // "0"')

case "$FORMAT" in
  json)
    echo "$RESPONSE" | jq '[.schema.fields[].name] as $cols
      | .rows // [] | .[]
      | [.f[].v] | to_entries
      | map({($cols[.key]): .value}) | add'
    ;;
  table)
    echo "$RESPONSE" | jq -r '
      [.schema.fields[].name] as $cols
      | ($cols | @tsv),
        (.rows // [] | .[] | [.f[].v] | @tsv)' | column -t -s $'\t'
    ;;
  csv)
    echo "$RESPONSE" | jq -r '
      [.schema.fields[].name] as $cols
      | ($cols | @csv),
        (.rows // [] | .[] | [.f[].v] | @csv)'
    ;;
esac

echo "---" >&2
echo "${TOTAL} rows" >&2
