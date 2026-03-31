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
make up

# クリーンアップ
make down
```

起動後の流れ:

1. **PostgreSQL** — DDL 実行 + シードデータ投入（EC サイトを模した約 2,000 件）
2. **BigQuery Emulator** — `raw` / `dwh` データセットを初期化
3. **EL (Python)** — PostgreSQL → BigQuery Emulator `raw` データセットにロード
4. **dbt** — `raw` → `dwh` に Star Schema を構築（staging → intermediate → marts）、テスト実行

## 便利コマンド（Makefile）

```bash
make help          # コマンド一覧を表示
```

### ライフサイクル

| コマンド | 説明 |
|----------|------|
| `make up` | 全サービスを起動（初回はビルド込み） |
| `make down` | 全サービスを停止しデータも削除 |
| `make restart` | クリーンスタート（down → up） |
| `make ps` | コンテナの状態を表示 |
| `make logs` | 全サービスのログを表示 |
| `make status` | dbt の実行結果サマリーを表示 |

### データベースアクセス

| コマンド | 説明 |
|----------|------|
| `make psql` | PostgreSQL に接続 |
| `make bq Q="SELECT ..."` | BigQuery Emulator にクエリ（テーブル形式） |
| `make bq-json Q="SELECT ..."` | 同上（JSON 形式） |
| `make bq-csv Q="SELECT ..."` | 同上（CSV 形式） |
| `make tables` | dwh データセットのテーブル一覧 |
| `make analysis` | 分析クエリ（先頭）を実行 |

### 個別実行

| コマンド | 説明 |
|----------|------|
| `make el-rerun` | EL パイプラインを再実行 |
| `make dbt-run` | dbt run を実行 |
| `make dbt-test` | dbt test を実行 |
| `make dbt-build` | dbt build（run + test）を実行 |

### Web UI

| コマンド | 説明 |
|----------|------|
| `make ui` | データ確認用の Web UI を起動 |
| `make ui-down` | Web UI を停止 |

`make ui` で http://localhost:3001 にアクセスすると、ブラウザ上で BigQuery Emulator のデータを確認できます。

- **Browse タブ** — データセット（dwh / raw）を切り替え、テーブル選択でスキーマ・データを閲覧
- **Query タブ** — SQL エディタで任意のクエリを実行（⌘+Enter）

通常の `make up` では起動されません（オプショナル）。

## プロジェクト構成

```
.
├── docker-compose.yml
├── Makefile                      # 便利コマンド集
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
├── queries/
│   └── analysis.sql              # 分析クエリ集（5 クエリ）
├── scripts/
│   └── bqquery.sh                # BigQuery クエリヘルパー
└── ui/                           # Web UI（React + TypeScript）
    ├── Dockerfile                # multi-stage: Node build → nginx serve
    ├── nginx.conf                # 静的配信 + BigQuery API プロキシ
    └── src/
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

```bash
# ヘルパースクリプトで実行
./scripts/bqquery.sh --table "SELECT d.year, d.month, SUM(f.net_amount) AS total FROM dwh.fact_sales f INNER JOIN dwh.dim_date d ON f.date_key = d.date_key GROUP BY 1, 2 ORDER BY 1, 2"

# または Makefile 経由
make bq Q="SELECT * FROM dwh.dim_customer LIMIT 5"
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
| Web UI | React 19 + TypeScript + Vite + Tailwind CSS |
| インフラ | Docker Compose |

## ポート一覧

| サービス | ホスト側ポート |
|----------|---------------|
| PostgreSQL | 15432 |
| BigQuery Emulator (REST) | 9050 |
| BigQuery Emulator (gRPC) | 9060 |
| Web UI | 3001 |
