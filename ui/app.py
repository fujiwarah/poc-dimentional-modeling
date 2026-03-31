"""BigQuery Emulator Data Explorer."""

import os

import pandas as pd
import streamlit as st
from google.auth.credentials import AnonymousCredentials
from google.cloud import bigquery

ENDPOINT = os.getenv("BIGQUERY_API_ENDPOINT", "http://localhost:9050")
PROJECT = os.getenv("BIGQUERY_PROJECT", "poc-project")


@st.cache_resource
def get_client() -> bigquery.Client:
    return bigquery.Client(
        project=PROJECT,
        credentials=AnonymousCredentials(),
        client_options={"api_endpoint": ENDPOINT},
    )


def run_query(sql: str) -> pd.DataFrame:
    client = get_client()
    rows = client.query(sql).result()
    return rows.to_dataframe()


def list_tables(dataset: str) -> list[str]:
    client = get_client()
    tables = client.list_tables(f"{PROJECT}.{dataset}")
    return sorted(t.table_id for t in tables)


def get_schema(dataset: str, table: str) -> list[dict]:
    client = get_client()
    t = client.get_table(f"{PROJECT}.{dataset}.{table}")
    return [{"column": f.name, "type": f.field_type, "mode": f.mode} for f in t.schema]


# --- UI ---

st.set_page_config(page_title="BQ Explorer", layout="wide")
st.title("BigQuery Emulator Explorer")

tab_browse, tab_query = st.tabs(["Browse", "Query"])

# --- Browse Tab ---
with tab_browse:
    col_ds, col_tbl = st.columns([1, 2])

    with col_ds:
        dataset = st.radio("Dataset", ["dwh", "raw"], horizontal=True)

    try:
        tables = list_tables(dataset)
    except Exception as e:
        st.error(f"接続エラー: {e}")
        st.stop()

    with col_tbl:
        table = st.selectbox("Table", tables)

    if table:
        schema_tab, data_tab = st.tabs(["Schema", "Data"])

        with schema_tab:
            schema = get_schema(dataset, table)
            st.dataframe(pd.DataFrame(schema), use_container_width=True, hide_index=True)

        with data_tab:
            limit = st.slider("Rows", min_value=10, max_value=500, value=100, step=10)
            df = run_query(f"SELECT * FROM `{PROJECT}.{dataset}.{table}` LIMIT {limit}")
            st.dataframe(df, use_container_width=True, hide_index=True)
            st.caption(f"{len(df)} rows")

# --- Query Tab ---
with tab_query:
    sql = st.text_area(
        "SQL",
        value="SELECT d.year, d.month, d.month_name,\n"
        "       COUNT(DISTINCT f.order_id) AS order_count,\n"
        "       SUM(f.net_amount) AS total_sales\n"
        "FROM dwh.fact_sales f\n"
        "INNER JOIN dwh.dim_date d ON f.date_key = d.date_key\n"
        "GROUP BY 1, 2, 3\n"
        "ORDER BY 1, 2",
        height=200,
    )
    if st.button("Run", type="primary"):
        try:
            df = run_query(sql)
            st.dataframe(df, use_container_width=True, hide_index=True)
            st.caption(f"{len(df)} rows")
        except Exception as e:
            st.error(str(e))
