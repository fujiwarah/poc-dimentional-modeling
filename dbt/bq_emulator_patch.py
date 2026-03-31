"""BigQuery Emulator 用 monkey patch。

dbt-bigquery が BigQuery Emulator に接続できるよう、以下をパッチする:
1. クライアント生成: AnonymousCredentials でエミュレータに接続
2. drop_relation: delete_table API の代わりに DROP SQL を使用
3. is_replaceable: 常に False (テーブル既存時)
4. _query_and_results: 全クエリを REST queries エンドポイント経由で実行
5. execute: DDL/SELECT 後処理をエミュレータ対応

BIGQUERY_EMULATOR_HOST 環境変数が設定されている場合のみ有効。
"""

import os
import re
import time
import json
import urllib.request
import urllib.error


def apply():
    """monkey patch を適用する。BIGQUERY_EMULATOR_HOST が未設定なら何もしない。"""
    emulator_host = os.environ.get("BIGQUERY_EMULATOR_HOST")
    if not emulator_host:
        return

    from google.auth.credentials import AnonymousCredentials
    from google.api_core.client_info import ClientInfo
    from google.api_core.client_options import ClientOptions
    from google.cloud.bigquery import Client as BigQueryClient

    import dbt.adapters.bigquery.__version__ as dbt_version
    import dbt.adapters.bigquery.clients as clients_mod

    endpoint = f"http://{emulator_host}"

    # --- パッチ 1: クライアント生成 ---
    def _patched_create_bigquery_client(credentials):
        return BigQueryClient(
            project=credentials.execution_project,
            credentials=AnonymousCredentials(),
            location=getattr(credentials, "location", None),
            client_info=ClientInfo(user_agent=f"dbt-bigquery-{dbt_version.version}"),
            client_options=ClientOptions(api_endpoint=endpoint),
        )

    clients_mod.create_bigquery_client = _patched_create_bigquery_client
    clients_mod._create_bigquery_client = _patched_create_bigquery_client

    import dbt.adapters.bigquery.connections as conn_mod
    conn_mod.create_bigquery_client = _patched_create_bigquery_client

    # --- パッチ 2: drop_relation ---
    import dbt.adapters.bigquery.impl as impl_mod

    def _patched_drop_relation(self, relation):
        is_cached = self._schema_is_cached(relation.database, relation.schema)
        if is_cached:
            self.cache_dropped(relation)
        fqn = f"`{relation.database}`.`{relation.schema}`.`{relation.identifier}`"
        for kind in ("TABLE", "VIEW"):
            try:
                _run_sync_query(endpoint, relation.database, f"DROP {kind} IF EXISTS {fqn}")
                return
            except Exception:
                continue

    impl_mod.BigQueryAdapter.drop_relation = _patched_drop_relation

    # --- パッチ 3: is_replaceable ---
    def _patched_is_replaceable(self, relation, partition_by=None, cluster_by=None):
        if relation is None:
            return True
        return False

    impl_mod.BigQueryAdapter.is_replaceable = _patched_is_replaceable

    # --- パッチ 4: 全クエリを REST queries API 経由に ---
    from dbt.adapters.events.types import SQLQueryStatus
    from dbt.adapters.bigquery.connections import BigQueryAdapterResponse
    from dbt_common.events.functions import fire_event
    from dbt_common.events.contextvars import get_node_info

    def _strip_comment(sql):
        return re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL).strip()

    def _patched_query_and_results(self, conn, sql, job_params, job_id, limit=None):
        """全クエリを REST queries API で同期実行する。"""
        project = conn.handle.project
        pre = time.perf_counter()

        result = _run_sync_query(endpoint, project, sql)

        if "error" in result:
            err_msg = result["error"].get("message", str(result["error"]))
            raise Exception(f"BigQuery Emulator error: {err_msg}")

        elapsed = time.perf_counter() - pre
        fire_event(
            SQLQueryStatus(
                status="OK",
                elapsed=elapsed,
                node_info=get_node_info(),
                query_id=job_id,
            )
        )

        # statement_type を推定
        stripped = _strip_comment(sql).upper().lstrip()
        if stripped.startswith("CREATE TABLE") or stripped.startswith("CREATE\n"):
            stmt_type = "CREATE_TABLE_AS_SELECT"
        elif stripped.startswith("CREATE VIEW"):
            stmt_type = "CREATE_VIEW"
        elif stripped.startswith("DROP"):
            stmt_type = "DROP_TABLE"
        elif stripped.startswith("INSERT"):
            stmt_type = "INSERT"
        elif stripped.startswith("SELECT") or stripped.startswith("WITH"):
            stmt_type = "SELECT"
        else:
            stmt_type = "SCRIPT"

        # FakeJob
        class _FakeJob:
            pass
        fake = _FakeJob()
        fake.job_id = job_id
        fake.project = project
        fake.location = "US"
        fake.total_bytes_billed = 0
        fake.total_bytes_processed = 0
        fake.slot_millis = 0
        fake.num_dml_affected_rows = 0
        fake.statement_type = stmt_type
        fake._is_emulator = True
        fake._result = result

        # イテレータ
        rows = result.get("rows", [])
        schema_fields = result.get("schema", {}).get("fields", [])

        class _SchemaField:
            """agate が期待する .name 属性を持つスキーマフィールド。"""
            def __init__(self, name, field_type="STRING", mode="NULLABLE"):
                self.name = name
                self.field_type = field_type
                self.mode = mode

        class _ResultIterator:
            def __init__(self, rows, schema_raw):
                self._rows = rows
                self._schema_raw = schema_raw
                self.total_rows = int(result.get("totalRows", len(rows)))
                # get_table_from_response が resp.schema を参照する
                self.schema = [
                    _SchemaField(
                        f.get("name", f"f{i}"),
                        f.get("type", "STRING"),
                        f.get("mode", "NULLABLE"),
                    )
                    for i, f in enumerate(schema_raw)
                ]

            def __iter__(self):
                for row in self._rows:
                    values = row.get("f", [])
                    yield _Row(values, self._schema_raw)

        def _cast_value(val, field_type):
            """BigQuery REST API の文字列値を Python 型に変換する。"""
            if val is None:
                return None
            field_type = (field_type or "STRING").upper()
            if field_type in ("INT64", "INTEGER", "NUMERIC", "FLOAT", "FLOAT64", "BIGNUMERIC"):
                try:
                    if "." in str(val):
                        return float(val)
                    return int(val)
                except (ValueError, TypeError):
                    return val
            if field_type == "BOOLEAN":
                return str(val).lower() in ("true", "1")
            return val

        class _Row:
            """agate が期待する dict-like かつ attribute access 可能な行。"""
            def __init__(self, values, schema):
                self._data = {}
                self._keys = []
                for i, field in enumerate(schema):
                    fname = field.get("name", f"f{i}")
                    ftype = field.get("type", "STRING")
                    raw = values[i]["v"] if i < len(values) else None
                    val = _cast_value(raw, ftype)
                    self._data[fname] = val
                    self._keys.append(fname)
                    setattr(self, fname, val)

            def __getitem__(self, key):
                if isinstance(key, int):
                    return self._data[self._keys[key]]
                return self._data[key]

            def values(self):
                return [self._data[k] for k in self._keys]

        return fake, _ResultIterator(rows, schema_fields)

    conn_mod.BigQueryConnectionManager._query_and_results = _patched_query_and_results

    # --- パッチ 5: execute() をエミュレータ対応 ---
    def _patched_execute(self, sql, auto_begin=False, fetch=None, limit=None):
        sql_with_comment = self._add_query_comment(sql)
        query_job, iterator = self.raw_execute(sql_with_comment, limit=limit)

        if fetch:
            table = self.get_table_from_response(iterator)
        else:
            from dbt_common.clients import agate_helper
            table = agate_helper.empty_table()

        message = "OK"
        code = None
        num_rows = None

        if query_job.statement_type == "CREATE_VIEW":
            code = "CREATE VIEW"
        elif query_job.statement_type == "CREATE_TABLE_AS_SELECT":
            code = "CREATE TABLE"
            # Emulator: REST API で行数を取得
            stripped = re.sub(r"/\*.*?\*/", "", sql_with_comment, flags=re.DOTALL).strip()
            m = re.search(r"create\s+table\s+`([^`]+)`\.`([^`]+)`\.`([^`]+)`",
                          stripped, re.IGNORECASE)
            if m:
                tbl = f"{m.group(2)}.{m.group(3)}"
                try:
                    r = _run_sync_query(endpoint, query_job.project,
                                        f"SELECT COUNT(*) as cnt FROM {tbl}")
                    num_rows = int(r["rows"][0]["f"][0]["v"])
                except Exception:
                    num_rows = 0
            else:
                num_rows = 0
        elif query_job.statement_type == "SCRIPT":
            code = "SCRIPT"
        elif query_job.statement_type in ["INSERT", "DELETE", "MERGE", "UPDATE"]:
            code = query_job.statement_type
            num_rows = query_job.num_dml_affected_rows
        elif query_job.statement_type == "SELECT":
            code = "SELECT"
            num_rows = iterator.total_rows
        elif query_job.statement_type == "DROP_TABLE":
            code = "DROP TABLE"

        bytes_processed = query_job.total_bytes_processed
        slot_ms = query_job.slot_millis
        processed_bytes = self.format_bytes(bytes_processed)

        if num_rows is not None:
            num_rows_formatted = self.format_rows_number(num_rows)
            message = f"{code} ({num_rows_formatted} rows, {processed_bytes} processed)"
        elif bytes_processed is not None:
            message = f"{code} ({processed_bytes} processed)"
        elif code:
            message = f"{code}"

        response = BigQueryAdapterResponse(
            _message=message,
            code=code,
            rows_affected=num_rows,
            bytes_processed=bytes_processed,
            bytes_billed=query_job.total_bytes_billed,
            location=query_job.location,
            project_id=query_job.project,
            job_id=query_job.job_id,
            slot_ms=slot_ms,
        )
        return response, table

    conn_mod.BigQueryConnectionManager.execute = _patched_execute

    print(f"[bq_emulator_patch] BigQuery Emulator パッチ適用済み: {endpoint}")


def _run_sync_query(endpoint, project, sql):
    """BigQuery Emulator の同期 queries エンドポイントで SQL を実行する。"""
    url = f"{endpoint}/bigquery/v2/projects/{project}/queries"
    data = json.dumps({"query": sql, "useLegacySql": False}).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=600) as resp:
        return json.loads(resp.read().decode("utf-8"))
