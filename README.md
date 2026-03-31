# PoC: ディメンショナルモデリングによる ELT パイプライン

PostgreSQL (OLTP) のトランザクションデータを、BigQuery Emulator 上で Kimball 式 Star Schema に変換する ELT パイプラインの PoC。

`docker compose up` 一発で、データソース構築からデータウェアハウス完成までが完結する。

## アーキテクチャ

```
PostgreSQL (OLTP)       Python EL           BigQuery Emulator         dbt-core
┌──────────────┐    ┌────────────┐    ┌──────────────────┐    ┌─────────────────┐
│ customers    │───>│            │───>│ raw.customers    │    │  staging        │
│ categories   │───>│  Extract   │───>│ raw.categories   │───>│  stg_*          │
│ products     │───>│  & Load    │───>│ raw.products     │    │       │         │
│ orders       │───>│            │───>│ raw.orders       │    │       v         │
│ order_items  │───>│            │───>│ raw.order_items  │    │  intermediate   │
└──────────────┘    └────────────┘    └──────────────────┘    │  int_*          │
                                                              │       │         │
                                                              │       v         │
                                                              │  marts          │
                                                              │  dim_* / fact_* │
                                                              └─────────────────┘
```

### Star Schema

```
                  ┌──────────────┐
                  │  dim_date    │
                  └──────┬───────┘
                         │
┌───────────────┐ ┌──────┴────────┐ ┌────────────────┐
│ dim_customer  ├─┤  fact_sales   ├─┤  dim_product    │
└───────────────┘ └──────┬────────┘ └────────────────┘
                         │
                  ┌──────┴───────────┐
                  │ dim_order_status │
                  └──────────────────┘
```

## 必要なもの

- Docker / Docker Compose

## クイックスタート

```bash
# 全工程を自動実行（初回はイメージビルドのため数分かかる）
docker compose up --build --wait

# クリーンアップ
docker compose down -v
```

起動後の流れ:

1. **PostgreSQL** — DDL 実行 + シードデータ投入（EC サイトを模した約 2,000 件）
2. **BigQuery Emulator** — `raw` / `dwh` データセットを初期化
3. **EL (Python)** — PostgreSQL → BigQuery Emulator `raw` データセットにロード
4. **dbt** — `raw` → `dwh` に Star Schema を構築（staging → intermediate → marts）、テスト実行

## プロジェクト構成

```
.
├── docker-compose.yml
├── .env                          # PostgreSQL 接続情報（PoC 用固定値）
├── postgres/
│   └── init/
│       ├── 01_ddl.sql            # OLTP スキーマ（5 テーブル）
│       └── 02_seed.sql           # シードデータ
├── bigquery/
│   └── dataset.yml               # BigQuery Emulator データセット定義
├── el/
│   ├── Dockerfile
│   ├── main.py                   # Extract & Load スクリプト
│   ├── config.py
│   └── requirements.txt
├── dbt/
│   ├── Dockerfile
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── run_dbt.py                # dbt CLI ラッパー
│   ├── bq_emulator_patch.py      # BigQuery Emulator 用 monkey patch
│   ├── macros/
│   ├── models/
│   │   ├── staging/              # raw → 型キャスト・クリーニング
│   │   ├── intermediate/         # JOIN・ビジネスロジック
│   │   └── marts/                # Star Schema（dim / fact）
│   └── tests/
└── queries/
    └── analysis.sql              # 分析クエリ集（5 クエリ）
```

## データモデル

### OLTP (PostgreSQL)

| テーブル | 件数 | 説明 |
|----------|------|------|
| customers | 100 | 顧客マスタ（日本人名・住所） |
| categories | 10 | 商品カテゴリ（2 階層） |
| products | 50 | 商品マスタ |
| orders | 500 | 注文ヘッダ（過去 1 年分） |
| order_items | ~1,500 | 注文明細 |

### OLAP (BigQuery Emulator — `dwh` データセット)

| テーブル | レイヤー | 説明 |
|----------|----------|------|
| stg_* | staging | raw データのクリーニング・型変換 |
| int_customers_with_region | intermediate | 顧客 + full_name + 都道府県→地方区分 |
| int_products_with_categories | intermediate | 商品 + カテゴリ・親カテゴリ非正規化 |
| int_orders_with_items | intermediate | 注文×明細 JOIN + 金額計算 |
| dim_date | marts | 日付ディメンション（365 日） |
| dim_customer | marts | 顧客ディメンション（8 地方区分） |
| dim_product | marts | 商品ディメンション |
| dim_order_status | marts | 注文ステータスディメンション（日本語ラベル） |
| fact_sales | marts | 売上ファクト（粒度: 注文明細 1 行） |

### ディメンショナルモデリングの主要概念

- **サロゲートキー** — 全ディメンションに `ROW_NUMBER()` で付与
- **デジェネレートディメンション** — `order_id`, `order_item_id` を fact に直接格納
- **計算済みメジャー** — `gross_amount`, `discount_amount`, `net_amount`
- **導出属性** — `prefecture` → `region`（47 都道府県 → 8 地方区分）

## 分析クエリ

`queries/analysis.sql` に 5 つの分析クエリを収録:

| # | クエリ | 使用ディメンション |
|---|--------|-------------------|
| 1 | 月別売上推移 | dim_date |
| 2 | 商品カテゴリ別売上ランキング | dim_product |
| 3 | 地方別・顧客セグメント分析 | dim_customer |
| 4 | 注文ステータス分析 | dim_order_status |
| 5 | 曜日別注文傾向 | dim_date |

BigQuery Emulator の REST API で実行:

```bash
curl -s -X POST http://localhost:9050/bigquery/v2/projects/poc-project/queries \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT d.year, d.month, SUM(f.net_amount) AS total FROM dwh.fact_sales f INNER JOIN dwh.dim_date d ON f.date_key = d.date_key GROUP BY 1, 2 ORDER BY 1, 2", "useLegacySql": false}' | jq '.rows'
```

## BigQuery Emulator との接続

dbt-bigquery は標準では BigQuery Emulator への接続をサポートしていない。本 PoC では `bq_emulator_patch.py` で以下の monkey patch を適用して解決:

| パッチ | 内容 |
|--------|------|
| クライアント生成 | `AnonymousCredentials` + カスタム `api_endpoint` |
| クエリ実行 | Jobs API → 同期 REST queries エンドポイント |
| テーブル削除 | `delete_table()` → SQL `DROP TABLE IF EXISTS` |
| テーブル作成 | `CREATE OR REPLACE` → `DROP + CREATE` |

## 技術スタック

| コンポーネント | 技術 |
|---------------|------|
| OLTP | PostgreSQL 16 |
| DWH | BigQuery Emulator (goccy/bigquery-emulator) |
| EL | Python 3.12 + psycopg2 + google-cloud-bigquery |
| Transform | dbt-core + dbt-bigquery |
| インフラ | Docker Compose |
