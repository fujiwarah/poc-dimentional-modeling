# PoC: ディメンショナルモデリングによる ELT パイプライン

PostgreSQL (OLTP) のトランザクションデータを、BigQuery Emulator 上で Kimball 式 Star Schema に変換する ELT パイプラインの PoC。

`docker compose up` 一発で、データソース構築からデータウェアハウス完成までが完結する。

## アーキテクチャ

```
PostgreSQL (OLTP)       Python EL           BigQuery Emulator         dbt-core
┌──────────────┐    ┌────────────┐    ┌──────────────────┐    ┌─────────────────┐
│ customers    │───>│            │───>│ raw.customers    │    │  staging (8)    │
│ categories   │───>│            │───>│ raw.categories   │───>│  stg_*          │
│ products     │───>│  Extract   │───>│ raw.products     │    │       │         │
│ orders       │───>│  & Load    │───>│ raw.orders       │    │       v         │
│ order_items  │───>│            │───>│ raw.order_items  │    │  intermediate(8)│
│ page_views   │───>│            │───>│ raw.page_views   │    │  int_*          │
│ inv_snapshots│───>│            │───>│ raw.inv_snapshots│    │       │         │
│ coupons      │───>│            │───>│ raw.coupons      │    │       v         │
└──────────────┘    └────────────┘    └──────────────────┘    │  marts (9)      │
                                                              │  dim_* / fact_* │
                                                              └─────────────────┘
```

### Star Schema

```
                    ┌─────────────┐
                    │  dim_date   │
                    └──────┬──────┘
                           │
┌──────────────┐    ┌──────┴──────┐    ┌──────────────┐
│ dim_customer │────│ fact_sales  │────│ dim_product   │
└──────┬───────┘    └──────┬──────┘    └──────┬────────┘
       │                   │                  │
       │            ┌──────┴──────┐    ┌──────┴──────────────┐
       │            │dim_order_   │    │fact_inventory_daily  │
       │            │  status     │    └─────────────────────┘
       │            └─────────────┘
       │            ┌─────────────┐
       │            │  dim_time   │
       │            └──────┬──────┘
       │                   │
       │            ┌──────┴──────────┐
       └────────────│ fact_page_views │────── dim_product
                    └─────────────────┘

共有ディメンション: dim_date, dim_time, dim_customer, dim_product
ファクト: fact_sales, fact_page_views, fact_inventory_daily
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

1. **PostgreSQL** — DDL 実行 + シードデータ投入（EC サイトを模した約 53,000 件。`scripts/generate_seed.py` で生成）
2. **BigQuery Emulator** — `raw` / `dwh` データセットを初期化
3. **EL (Python)** — PostgreSQL → BigQuery Emulator `raw` データセットにロード（8 テーブル）
4. **dbt** — `raw` → `dwh` に Star Schema を構築（25 モデル + 73 テスト）

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
| `make build` | イメージのリビルドのみ（起動しない） |
| `make ps` | コンテナの状態を表示 |
| `make logs` | 全サービスのログを表示 |
| `make logs-el` | EL サービスのログのみ表示 |
| `make logs-dbt` | dbt サービスのログのみ表示 |
| `make status` | dbt の実行結果サマリーを表示 |
| `make clean` | コンテナ・ボリューム・ビルドキャッシュを削除 |

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

- **Browse タブ** — データセット（dwh / raw）を切り替え、テーブル選択でスキーマ・データを閲覧（ページネーション・ソート対応）
- **Query タブ** — SQL エディタで任意のクエリを実行（⌘+Enter）。テンプレート・実行履歴・テーブル/チャート表示切替
- **Schema タブ** — ReactFlow によるインタラクティブなスキーマ可視化。テーブル種別（Fact/Dimension/Staging/Intermediate）の色分け、PK/FK/メジャー等のカラムロールバッジ、レイヤー別フィルタリング
- **Dashboard タブ** — 8 種の事前構築済み分析チャート（月別売上推移、カテゴリ別売上、RFM セグメント分布、ABC 分析、ファネル分析、地域別売上、時間帯別注文パターン、決済手段構成）
- **Training タブ** — ディメンショナルモデリングのインタラクティブ学習。レッスン形式のテキスト・図解・SQL 演習問題

通常の `make up` では起動されません（オプショナル）。

## プロジェクト構成

```
.
├── docker-compose.yml
├── Makefile                      # 便利コマンド集
├── .env                          # PostgreSQL 接続情報（PoC 用固定値）
├── postgres/
│   └── init/
│       ├── 01_ddl.sql            # OLTP スキーマ（8 テーブル）
│       └── 02_seed.sql           # シードデータ（generate_seed.py で生成）
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
│   ├── uc01_*.sql ... uc12_*.sql # 分析クエリ集（12 ユースケース）
│   ├── validation/               # データ品質検証クエリ（7 件）
│   └── analysis.sql              # レガシー分析クエリ
├── scripts/
│   ├── generate_seed.py          # シードデータ生成スクリプト
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
| customers | 1,000 | 顧客マスタ（日本人名・住所・性別・生年月日・登録チャネル） |
| categories | 10 | 商品カテゴリ（2 階層） |
| products | 80 | 商品マスタ（原価・重量・販売フラグ） |
| orders | 5,000 | 注文ヘッダ（2 年分・決済方法・デバイス・クーポン） |
| order_items | ~14,500 | 注文明細 |
| page_views | ~25,500 | ページビュー（アクセスログ） |
| inventory_snapshots | ~7,300 | 日次在庫スナップショット |
| coupons | 30 | クーポンマスタ |

データ量は `scripts/generate_seed.py` の定数で調整可能（乱数シード固定で再現性あり）。

### OLAP (BigQuery Emulator — `dwh` データセット)

| テーブル | レイヤー | 説明 |
|----------|----------|------|
| stg_* (8) | staging | raw データのクリーニング・型変換 |
| int_customers_with_region | intermediate | 顧客 + full_name + 地方区分 + 性別・年代 |
| int_products_with_categories | intermediate | 商品 + カテゴリ非正規化 + 粗利率 |
| int_orders_with_items | intermediate | 注文×明細 JOIN + 金額計算 + 決済・デバイス |
| int_customer_rfm | intermediate | RFM スコアリング（閾値ベース 1-5） |
| int_customer_ltv | intermediate | 顧客 LTV 推定 |
| int_product_abc | intermediate | 商品 ABC 分析ランク |
| int_page_view_sessions | intermediate | セッション集計・ファネル分析 |
| int_inventory_with_sales | intermediate | 在庫 + 販売データ結合・在庫日数 |
| dim_date | marts | 日付ディメンション（会計年度・祝日フラグ） |
| dim_time | marts | 時間帯ディメンション（24 行） |
| dim_customer | marts | 顧客ディメンション（RFM セグメント・LTV） |
| dim_product | marts | 商品ディメンション（ABC ランク・粗利率） |
| dim_order_status | marts | 注文ステータスディメンション（日本語ラベル） |
| dim_coupon | marts | クーポンディメンション |
| fact_sales | marts | 売上ファクト（粒度: 注文明細 1 行・粗利含む） |
| fact_page_views | marts | ページビューファクト（粒度: 1 PV） |
| fact_inventory_daily | marts | 日次在庫ファクト（粒度: 商品×日） |

### ディメンショナルモデリングの主要概念

- **サロゲートキー** — ディメンションに付与（`customer_key`, `product_key` 等）
- **デジェネレートディメンション** — `order_id`, `order_item_id` を fact に直接格納
- **計算済みメジャー** — `gross_amount`, `discount_amount`, `net_amount`, `gross_profit`
- **導出属性** — `prefecture` → `region`、`birth_date` → `age_group`、RFM スコア
- **複数ファクトテーブル** — 売上・ページビュー・在庫の 3 ファクト
- **共有ディメンション** — dim_date, dim_time, dim_customer, dim_product を複数ファクトで共有

## 分析クエリ

`queries/` に 12 の分析ユースケース + 7 の検証クエリを収録:

### 分析ユースケース（`queries/uc*.sql`）

| # | ファイル | 分析内容 |
|---|----------|---------|
| UC-1 | `uc01_monthly_sales_yoy.sql` | 月次売上推移と前年同月比 |
| UC-2 | `uc02_moving_average_7d.sql` | 7 日移動平均 |
| UC-3 | `uc03_hourly_sales.sql` | 時間帯別売上 |
| UC-4 | `uc04_rfm_segment_sales.sql` | RFM セグメント別売上 |
| UC-5 | `uc05_cohort_retention.sql` | コホート分析（月次リテンション） |
| UC-6 | `uc06_customer_ltv_distribution.sql` | 顧客 LTV 分布 |
| UC-7 | `uc07_abc_analysis.sql` | ABC 分析 |
| UC-8 | `uc08_cross_sell.sql` | クロスセル分析 |
| UC-9 | `uc09_conversion_funnel.sql` | コンバージョンファネル |
| UC-10 | `uc10_view_to_purchase.sql` | 閲覧→購入コンバージョン率 |
| UC-11 | `uc11_inventory_turnover.sql` | 在庫回転率 |
| UC-12 | `uc12_stockout_analysis.sql` | 欠品分析 |

### データ検証クエリ（`queries/validation/`）

生成データの分布が設計通りか確認するクエリ（顧客セグメント、性別・年代、ステータス、決済・デバイス、季節性、ファネル変換率、欠品）。

```bash
# クエリ実行例
make bq Q="$(cat queries/uc01_monthly_sales_yoy.sql)"

# または直接
make bq Q="SELECT * FROM dwh.dim_customer LIMIT 5"
```

### シードデータの再生成

```bash
# データ量を変更したい場合は scripts/generate_seed.py の定数を編集してから:
python scripts/generate_seed.py
make restart
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
| Web UI | React 19 + TypeScript 5 + Vite 8 + Tailwind CSS |
| UI ライブラリ | Recharts（チャート）, ReactFlow（スキーマ可視化）, CodeMirror 6（SQL エディタ）, TanStack Table（テーブル表示） |
| インフラ | Docker Compose |

## ポート一覧

| サービス | ホスト側ポート |
|----------|---------------|
| PostgreSQL | 15432 |
| BigQuery Emulator (REST) | 9050 |
| BigQuery Emulator (gRPC) | 9060 |
| Web UI | 3001 |
