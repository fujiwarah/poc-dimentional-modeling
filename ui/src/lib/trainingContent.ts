export interface ExerciseSection {
  type: "exercise";
  title: string;
  description: string;
  sql: string;
  hint?: string;
}

export interface TextSection {
  type: "text";
  content: string;
}

export interface DiagramSection {
  type: "diagram";
  content: string;
}

export type Section = TextSection | DiagramSection | ExerciseSection;

export interface Lesson {
  id: string;
  title: string;
  subtitle: string;
  sections: Section[];
}

export const LESSONS: Lesson[] = [
  {
    id: "what-is-dimensional-modeling",
    title: "1. ディメンショナルモデリングとは",
    subtitle: "OLTPとOLAPの違い、分析のためのデータモデル",
    sections: [
      {
        type: "text",
        content: `## ディメンショナルモデリングとは？

ディメンショナルモデリングは、**分析・レポーティングに最適化されたデータモデリング手法**です。Ralph Kimball が提唱し、データウェアハウス（DWH）設計の標準的なアプローチとして広く採用されています。

### なぜ必要なのか？

業務システム（OLTP）のデータベースは、日々のトランザクション処理に最適化されています。正規化されたテーブル構造は、データの整合性を保ち、更新処理を効率的に行えますが、分析クエリには向きません。

| 観点 | OLTP（業務系） | OLAP（分析系） |
|------|--------------|--------------|
| **目的** | トランザクション処理 | データ分析・レポート |
| **正規化** | 高度に正規化（3NF） | 非正規化（スター型） |
| **クエリ** | 単一レコードの読み書き | 大量データの集計 |
| **JOIN** | 多数のテーブルを結合 | 少数のテーブルを結合 |
| **ユーザー** | アプリケーション | アナリスト・BI ツール |

### OLTP テーブルの例

ECサイトの業務DBでは、注文情報が複数テーブルに正規化されています。`,
      },
      {
        type: "diagram",
        content: `+----------------+     +----------------+     +----------------+
|   orders       |---->| order_items    |---->|  products      |
+----------------+     +----------------+     +----------------+
| order_id    PK |     | item_id     PK |     | product_id  PK |
| customer_id    |     | order_id    FK |     | name           |
| ordered_at     |     | product_id  FK |     | category_id    |
| status         |     | quantity       |     | price          |
+-------+--------+     | unit_price     |     +----------------+
        |              +----------------+
        v                                     +----------------+
+----------------+                            | categories     |
|  customers     |                            +----------------+
+----------------+                            | category_id    |
| customer_id    |                            | name           |
| name           |                            | parent_id      |
| email          |                            +----------------+
+----------------+

          OLTP: 正規化された構造 (3NF)
            JOIN が多く、分析クエリが複雑になりがち`,
      },
      {
        type: "text",
        content: `### ディメンショナルモデルに変換すると？

上記の正規化されたテーブル群を、分析に適した形に再構成します。中心に**ファクトテーブル**（計測値・イベント）を配置し、周囲に**ディメンションテーブル**（分析の切り口）を配置します。

これにより、分析クエリが直感的でシンプルになります。`,
      },
      {
        type: "diagram",
        content: `                    +----------------+
                    |   dim_date     |
                    +----------------+
                    | date_key    PK |
                    | year, month    |
                    | quarter        |
                    | day_of_week    |
                    +-------+--------+
                            |
+----------------+   +------+--------+   +----------------+
| dim_customer   |   |  fact_sales   |   | dim_product    |
+----------------+   +---------------+   +----------------+
| customer_key   |<--| customer_key  |   | product_key    |
| name           |   | product_key   |-->| name           |
| region         |   | date_key      |   | category       |
| segment        |   +---------------+   | price          |
+----------------+   | quantity      |   +----------------+
                     | net_amount    |
                     | gross_profit  |
                     +---------------+

          OLAP: スタースキーマ (星型)
            JOIN が少なく、分析クエリがシンプル`,
      },
      {
        type: "exercise",
        title: "演習: OLTPとOLAPを比較する",
        description:
          "OLTPテーブル（raw データセット）と、変換後のスタースキーマ（dwh データセット）のテーブル一覧を見比べてみましょう。まずはOLTPテーブルの構造を確認します。",
        sql: `-- raw（OLTP由来）のテーブル一覧と行数を確認
SELECT 'orders' AS table_name, COUNT(*) AS row_count FROM raw.orders
UNION ALL
SELECT 'order_items', COUNT(*) FROM raw.order_items
UNION ALL
SELECT 'products', COUNT(*) FROM raw.products
UNION ALL
SELECT 'customers', COUNT(*) FROM raw.customers
UNION ALL
SELECT 'categories', COUNT(*) FROM raw.categories
ORDER BY table_name`,
      },
      {
        type: "exercise",
        title: "演習: DWHテーブルを確認する",
        description:
          "次に、ディメンショナルモデルに変換されたDWHテーブルを確認します。ファクトとディメンションに整理されていることに注目してください。",
        sql: `-- dwh（スタースキーマ）のテーブル一覧と行数を確認
SELECT 'fact_sales' AS table_name, 'fact' AS type, COUNT(*) AS row_count FROM dwh.fact_sales
UNION ALL
SELECT 'fact_page_views', 'fact', COUNT(*) FROM dwh.fact_page_views
UNION ALL
SELECT 'fact_inventory_daily', 'fact', COUNT(*) FROM dwh.fact_inventory_daily
UNION ALL
SELECT 'dim_customer', 'dimension', COUNT(*) FROM dwh.dim_customer
UNION ALL
SELECT 'dim_product', 'dimension', COUNT(*) FROM dwh.dim_product
UNION ALL
SELECT 'dim_date', 'dimension', COUNT(*) FROM dwh.dim_date
UNION ALL
SELECT 'dim_time', 'dimension', COUNT(*) FROM dwh.dim_time
UNION ALL
SELECT 'dim_order_status', 'dimension', COUNT(*) FROM dwh.dim_order_status
UNION ALL
SELECT 'dim_coupon', 'dimension', COUNT(*) FROM dwh.dim_coupon
ORDER BY type, table_name`,
      },
      {
        type: "text",
        content: `### まとめ

- **OLTP**（業務系）は正規化されており、データの整合性を保つのに適している
- **OLAP**（分析系）は非正規化されており、分析クエリを効率的に実行できる
- ディメンショナルモデリングは OLTP → OLAP の変換手法
- 中心に **ファクト**（計測値）、周囲に **ディメンション**（分析軸）を配置する`,
      },
    ],
  },
  {
    id: "fact-and-dimension",
    title: "2. ファクトとディメンション",
    subtitle: "ファクトテーブルとディメンションテーブルの役割",
    sections: [
      {
        type: "text",
        content: `## ファクトテーブル

ファクトテーブルは、**ビジネスプロセスの計測値（メトリクス）** を格納するテーブルです。「何が起きたか」を数値で記録します。

### ファクトテーブルの特徴

- **粒度（Grain）**: 1行が何を表すかを明確に定義する（例：「1注文1商品の1行」）
- **計測値（Measures）**: 集計可能な数値カラム（金額、数量、利益など）
- **外部キー（FK）**: ディメンションテーブルへの参照キー
- **行数**: 通常、最も多い（トランザクション数に比例）

### ファクトの種類

| 種類 | 説明 | 例 |
|------|------|-----|
| **トランザクションファクト** | 個々のイベントを記録 | 注文明細、ページビュー |
| **定期スナップショットファクト** | 定期的な状態を記録 | 日次在庫、月次残高 |
| **累積スナップショットファクト** | プロセスの進捗を記録 | 注文の受注→出荷→配達 |

## ディメンションテーブル

ディメンションテーブルは、**分析の切り口（コンテキスト）** を提供するテーブルです。「誰が」「いつ」「何を」「どこで」に答えます。

### ディメンションテーブルの特徴

- **サロゲートキー**: ビジネスキーとは別の、DWH用の一意な識別子
- **属性（Attributes）**: フィルタやグルーピングに使うカラム
- **説明的な情報**: ラベルや名称など、人間が理解しやすい情報
- **行数**: ファクトに比べて少ない（マスタデータ的）`,
      },
      {
        type: "exercise",
        title: "演習: ファクトテーブルの構造を確認する",
        description:
          "fact_sales テーブルの内容を見て、計測値（Measures）と外部キー（FK）を区別してみましょう。quantity, net_amount, gross_profit が計測値、*_key が外部キーです。",
        sql: `-- fact_sales の先頭10行を確認
SELECT
  sales_key,
  date_key, time_key, customer_key, product_key,
  order_id, order_item_id,
  quantity, unit_price, discount_rate,
  net_amount, gross_profit
FROM dwh.fact_sales
LIMIT 10`,
      },
      {
        type: "exercise",
        title: "演習: ディメンションテーブルの構造を確認する",
        description:
          "dim_product テーブルの内容を見て、サロゲートキー（product_key）と属性（商品名、カテゴリ名など）を確認しましょう。",
        sql: `-- dim_product の全カラムを確認
SELECT *
FROM dwh.dim_product
LIMIT 10`,
      },
      {
        type: "exercise",
        title: "演習: 3種類のファクトを確認する",
        description:
          "このDWHには3種類のファクトテーブルがあります。それぞれの粒度（1行が何を表すか）と計測値を確認しましょう。",
        sql: `-- 各ファクトテーブルのサンプルを比較
-- 1. トランザクションファクト: 注文明細（1行 = 1注文1商品）
SELECT 'fact_sales' AS fact_table, sales_key, order_id, quantity, net_amount
FROM dwh.fact_sales LIMIT 3`,
        hint: "fact_page_views（ページビュー）や fact_inventory_daily（日次在庫）も確認してみましょう。Queryタブで自由にSQLを実行できます。",
      },
      {
        type: "text",
        content: `### デジェネレートディメンション

fact_sales の \`order_id\` や \`order_item_id\` に注目してください。これらはディメンションキーではなく、ファクトテーブルに直接格納された識別子です。

独立したディメンションテーブルを持たないが、グルーピングやフィルタリングに使う属性を **デジェネレートディメンション（退化ディメンション）** と呼びます。注文番号のような値が典型例です。

### まとめ

- **ファクト** = 「何が起きたか」の数値（計測値 + 外部キー）
- **ディメンション** = 「誰が・いつ・何を」の文脈（属性 + サロゲートキー）
- ファクトは行数が多く、ディメンションは行数が少ない
- 粒度（Grain）の定義がファクト設計の最重要ポイント`,
      },
    ],
  },
  {
    id: "star-vs-snowflake",
    title: "3. スタースキーマ vs スノーフレーク",
    subtitle: "2つの主要なスキーマ設計パターンの比較",
    sections: [
      {
        type: "text",
        content: `## スタースキーマ（Star Schema）

スタースキーマは、ディメンショナルモデリングの最も基本的なパターンです。中心にファクトテーブル、周囲にディメンションテーブルを配置し、**星型** の構造を形成します。

### 特徴
- ディメンションテーブルは**非正規化**（フラット）
- カテゴリの階層はディメンション内にカラムとして持つ
- JOINの段数が少なく、クエリがシンプル
- BIツールとの相性が良い`,
      },
      {
        type: "diagram",
        content: `+--------------------+
|   dim_product      |
+--------------------+
| product_key     PK |
| product_name       |
| category_name      |  <-- 階層がフラットに
| parent_category    |      展開されている
| price              |
| gross_margin_pct   |
+---------+----------+
          |
          |   +---------------+
          |   | fact_sales    |
          +-->+---------------+<--- dim_date
              | product_key   |<--- dim_customer
              | quantity      |
              | net_amount    |
              +---------------+

  スタースキーマ: ディメンションがフラット`,
      },
      {
        type: "text",
        content: `## スノーフレークスキーマ（Snowflake Schema）

スノーフレークスキーマでは、ディメンションテーブルが**正規化**されます。階層的な属性（カテゴリ → サブカテゴリなど）を別テーブルに分離します。`,
      },
      {
        type: "diagram",
        content: `+-----------------+     +--------------------+
| parent_         |     |   dim_product      |
|  categories     |     +--------------------+
+-----------------+<----| product_key     PK |
| parent_cat_id   |     | product_name       |
| name            |     | category_id     FK |  <-- 正規化されて
+-----------------+     | price              |      別テーブルに分離
                        +---------+----------+
+-----------------+               |
|  categories     |               |   +---------------+
+-----------------+               |   | fact_sales    |
| category_id     |               +-->+---------------+
| name            |                   | product_key   |
| parent_cat_id   |                   | quantity      |
+-----------------+                   | net_amount    |
                                      +---------------+

  スノーフレーク: ディメンションが正規化`,
      },
      {
        type: "text",
        content: `## 比較

| 観点 | スタースキーマ | スノーフレーク |
|------|--------------|---------------|
| **ディメンションの正規化** | 非正規化（フラット） | 正規化（分割） |
| **JOINの数** | 少ない | 多い |
| **クエリの複雑さ** | シンプル | やや複雑 |
| **ストレージ** | やや冗長 | 効率的 |
| **BIツール互換性** | 高い | やや低い |
| **メンテナンス** | 属性更新が広範囲 | 一元管理しやすい |

### Kimball の推奨

Kimball は **スタースキーマを強く推奨** しています。理由は：

1. **クエリパフォーマンス**: JOINが少ないため高速
2. **理解しやすさ**: ビジネスユーザーが直感的に理解できる
3. **BI ツール**: ほとんどのBIツールがスター型に最適化されている

現代のDWH（BigQuery, Snowflake, Redshift）ではストレージコストが低いため、スノーフレーク型にする理由がさらに少なくなっています。`,
      },
      {
        type: "exercise",
        title: "演習: スタースキーマのシンプルさを体験する",
        description:
          "このDWHはスタースキーマで設計されています。カテゴリ別の売上をJOIN1つで取得できることを確認しましょう。もしスノーフレーク型だった場合、categories テーブルと parent_categories テーブルへのJOINが追加で必要になります。",
        sql: `-- スタースキーマ: 1回のJOINでカテゴリ階層付き集計ができる
SELECT
  p.parent_category_name,
  p.category_name,
  COUNT(*) AS sales_count,
  SUM(f.net_amount) AS total_sales
FROM dwh.fact_sales f
JOIN dwh.dim_product p ON f.product_key = p.product_key
GROUP BY 1, 2
ORDER BY 1, total_sales DESC`,
      },
      {
        type: "text",
        content: `### まとめ

- **スタースキーマ**: ディメンションをフラットに持つ。シンプルで高速。Kimballの推奨
- **スノーフレーク**: ディメンションを正規化する。ストレージ効率は良いが複雑
- 実務では **スタースキーマが主流**。特にクラウドDWHでは非正規化のコストが低い
- このプロジェクトもスタースキーマを採用している（dim_product にカテゴリ名が直接含まれている）`,
      },
    ],
  },
  {
    id: "scd",
    title: "4. SCD（緩やかに変化するディメンション）",
    subtitle: "ディメンション属性の変更をどう扱うか",
    sections: [
      {
        type: "text",
        content: `## SCD（Slowly Changing Dimension）とは？

ディメンションの属性は時間とともに変化します。例えば：

- 顧客の住所が変わった
- 商品のカテゴリが変更された
- 従業員の部署が異動になった

これらの変化を **どのように記録するか** が SCD の問題です。

## SCD のタイプ

### Type 0: 変更しない（固定）
属性を一度設定したら更新しない。初回の値を永久に保持する。

**用途**: 本当に変わらない属性（生年月日、初回登録日など）

### Type 1: 上書き（履歴なし）
新しい値で単純に上書きする。過去の値は失われる。

**用途**: 誤りの修正、分析に履歴が不要な属性`,
      },
      {
        type: "diagram",
        content: `SCD Type 1: 上書き

更新前:
  customer_key | name       | prefecture
  -------------|------------|----------
  1            | 田中太郎   | 東京都

更新後 (引越し):
  customer_key | name       | prefecture
  -------------|------------|----------
  1            | 田中太郎   | 大阪府      <-- 上書き (東京の履歴は消える)`,
      },
      {
        type: "text",
        content: `### Type 2: 新行追加（完全な履歴）
変更のたびに新しい行を追加する。有効期間（valid_from, valid_to）と現在フラグ（is_current）を持つ。

**用途**: 履歴を正確に追跡したい属性（顧客セグメント、商品カテゴリなど）`,
      },
      {
        type: "diagram",
        content: `SCD Type 2: 新行追加

  customer_key | name       | prefecture | valid_from | valid_to   | is_current
  -------------|------------|------------|------------|------------|----------
  1            | 田中太郎   | 東京都     | 2023-01-01 | 2024-06-30 | false
  2            | 田中太郎   | 大阪府     | 2024-07-01 | 9999-12-31 | true

  同じ顧客だがサロゲートキーが異なる
  ファクトは「注文時点の」customer_key を参照するため
  引越し前の注文は key=1、引越し後は key=2 に紐づく`,
      },
      {
        type: "text",
        content: `### Type 3: 新カラム追加（限定的な履歴）
「前の値」と「現在の値」を別カラムに保持する。直前の状態だけ追跡できる。

**用途**: 直前の状態との比較が必要だが、完全な履歴は不要な場合

### 実務での選択基準

| 基準 | Type 1 | Type 2 | Type 3 |
|------|--------|--------|--------|
| 履歴の保持 | なし | 完全 | 直前のみ |
| テーブルの行数 | 変わらない | 増える | 変わらない |
| 実装の複雑さ | 簡単 | やや複雑 | 簡単 |
| 一般的な用途 | 誤り修正 | セグメント変更 | 組織変更 |

実務で最もよく使うのは **Type 1 と Type 2 の組み合わせ** です。変化を追跡したい属性には Type 2、それ以外は Type 1 を適用します。`,
      },
      {
        type: "exercise",
        title: "演習: 現在のDWHのSCDタイプを確認する",
        description:
          "このプロジェクトの dim_customer は Type 1（上書き）で実装されています。valid_from/valid_to のようなカラムがないことを確認しましょう。",
        sql: `-- dim_customer の構造を確認（Type 1: サロゲートキー + 現在の属性のみ）
SELECT
  customer_key,
  customer_id,
  full_name,
  prefecture,
  region,
  rfm_segment
FROM dwh.dim_customer
LIMIT 10`,
        hint: "Type 2 を実装する場合、valid_from, valid_to, is_current カラムを追加し、変更のたびに新行を挿入する仕組みが必要になります。",
      },
      {
        type: "text",
        content: `### まとめ

- **SCD** はディメンション属性の変更管理手法
- **Type 1**（上書き）が最もシンプル。履歴不要な属性に使う
- **Type 2**（新行追加）が最も強力。完全な履歴追跡が可能
- 実務では Type 1 と Type 2 を属性ごとに使い分ける
- このプロジェクトは簡易化のため全属性 Type 1 を採用している`,
      },
    ],
  },
  {
    id: "conformed-dimension",
    title: "5. コンフォームドディメンション",
    subtitle: "複数のファクトテーブルで共有するディメンション",
    sections: [
      {
        type: "text",
        content: `## コンフォームドディメンションとは？

コンフォームドディメンションは、**複数のファクトテーブルで共有されるディメンション** です。Kimball アーキテクチャの最も重要な概念の一つです。

### なぜ重要か？

複数のビジネスプロセス（売上、在庫、アクセスログなど）のデータを **横断的に分析** するためには、同じディメンションを共有する必要があります。

例えば「商品Aの売上と在庫の関係」を分析するには、fact_sales と fact_inventory_daily が **同じ dim_product** を参照している必要があります。`,
      },
      {
        type: "diagram",
        content: `                   +----------------+
              +--->|   dim_date     |<---+
              |    +----------------+    |
              |                          |
  +-----------+----+     +---------------+-----------+
  |  fact_sales    |     | fact_inventory_daily       |
  +----------------+     +---------------------------+
  | date_key       |     | date_key                  |
  | product_key    |     | product_key               |
  | customer_key   |     | quantity_on_hand           |
  | net_amount     |     | days_of_supply             |
  +-----------+----+     +---------------+-----------+
              |                          |
              |    +----------------+    |
              +--->|  dim_product   |<---+
                   +----------------+

  dim_date と dim_product が2つのファクトで共有されている
  = コンフォームドディメンション`,
      },
      {
        type: "text",
        content: `### コンフォームドディメンションの条件

1. **同じサロゲートキー体系** を使う（同じ product_key が同じ商品を指す）
2. **同じ属性と定義** を持つ（"カテゴリ" の意味が統一されている）
3. **一箇所で管理** される（各ファクトが独自のディメンションを持たない）

### アンチパターン: 非コンフォームドディメンション

各部門やチームが独自のディメンションを作ってしまうと、横断分析ができなくなります。

- 売上チームの "商品マスタ" と在庫チームの "商品マスタ" のIDが異なる
- "カテゴリ" の分類基準が部門ごとに違う
- 同じ顧客に異なるキーが割り当てられている`,
      },
      {
        type: "exercise",
        title: "演習: コンフォームドディメンションを体験する",
        description:
          "dim_product を使って、売上（fact_sales）と在庫（fact_inventory_daily）を同時に分析してみましょう。同じ product_key でJOINできることがポイントです。",
        sql: `-- 商品別の売上と在庫状況を横断分析
SELECT
  p.product_name,
  p.category_name,
  sales.total_sales,
  sales.total_qty_sold,
  inv.latest_stock
FROM dwh.dim_product p
LEFT JOIN (
  SELECT product_key,
    SUM(net_amount) AS total_sales,
    SUM(quantity) AS total_qty_sold
  FROM dwh.fact_sales
  GROUP BY 1
) sales ON p.product_key = sales.product_key
LEFT JOIN (
  SELECT fi.product_key,
    fi.quantity_on_hand AS latest_stock
  FROM dwh.fact_inventory_daily fi
  JOIN dwh.dim_date d ON fi.date_key = d.date_key
  WHERE d.full_date = (
    SELECT MAX(d2.full_date)
    FROM dwh.fact_inventory_daily fi2
    JOIN dwh.dim_date d2 ON fi2.date_key = d2.date_key
  )
) inv ON p.product_key = inv.product_key
ORDER BY sales.total_sales DESC
LIMIT 15`,
      },
      {
        type: "exercise",
        title: "演習: dim_date の共有を確認する",
        description:
          "dim_date も複数ファクトで共有されるコンフォームドディメンションです。同じ date_key を使って、日付軸で異なるファクトを突き合わせられます。",
        sql: `-- 月別の売上件数とページビュー数を並べて確認
SELECT
  d.year, d.month, d.month_name,
  COUNT(DISTINCT fs.sales_key) AS sales_count,
  COUNT(DISTINCT fp.page_view_key) AS page_view_count
FROM dwh.dim_date d
LEFT JOIN dwh.fact_sales fs ON d.date_key = fs.date_key
LEFT JOIN dwh.fact_page_views fp ON d.date_key = fp.date_key
GROUP BY 1, 2, 3
ORDER BY 1, 2`,
      },
      {
        type: "text",
        content: `### まとめ

- **コンフォームドディメンション** = 複数ファクトで共有されるディメンション
- 横断分析（ドリルアクロス）の基盤となる最重要概念
- サロゲートキー体系と属性定義の統一が必須
- dim_date, dim_product のように、ビジネス全体で共通のマスタを一元管理する`,
      },
    ],
  },
  {
    id: "grain",
    title: "6. 粒度の決定",
    subtitle: "ファクトテーブル設計の最重要ポイント",
    sections: [
      {
        type: "text",
        content: `## 粒度（Grain）とは？

粒度とは、**ファクトテーブルの1行が何を表すか** を明確に定義したものです。ファクトテーブル設計において **最も重要な決定事項** です。

### なぜ重要か？

粒度を誤ると：
- 必要な詳細が失われる（粒度が粗すぎる）
- データ量が爆発する（粒度が細かすぎる）
- 計測値の二重カウントが発生する（粒度が不明確）

### 粒度の宣言例

| ファクト | 粒度の宣言 |
|---------|-----------|
| fact_sales | 「1つの注文の、1つの商品の、1つの行」 |
| fact_page_views | 「1人のユーザーの、1つのページの、1回の閲覧」 |
| fact_inventory_daily | 「1つの商品の、1日の、在庫スナップショット」 |

### 粒度決定の3ステップ

1. **ビジネスプロセスを選ぶ**: どのプロセスを計測するか（売上、在庫、アクセス）
2. **粒度を宣言する**: 1行が何を表すかを日本語で書く
3. **ディメンションを特定する**: その粒度に対して適用可能な分析軸を決める`,
      },
      {
        type: "exercise",
        title: "演習: 粒度の違いを確認する",
        description:
          "fact_sales の粒度は「注文×商品」です。1つの注文に複数商品がある場合、複数行になります。order_id でグルーピングすると注文単位に集約できます。",
        sql: `-- 注文ごとの明細行数を確認（粒度 = 注文×商品）
SELECT
  order_id,
  COUNT(*) AS line_items,
  SUM(quantity) AS total_qty,
  SUM(net_amount) AS order_total
FROM dwh.fact_sales
GROUP BY order_id
ORDER BY line_items DESC
LIMIT 10`,
        hint: "1つの注文に複数行があることに注目。これが「注文×商品」という粒度の意味です。",
      },
      {
        type: "exercise",
        title: "演習: 定期スナップショットの粒度",
        description:
          "fact_inventory_daily の粒度は「商品×日」です。同じ商品でも日付ごとに1行存在します。",
        sql: `-- 1つの商品の在庫推移（粒度 = 商品×日）
SELECT
  d.full_date,
  p.product_name,
  fi.quantity_on_hand,
  fi.quantity_available,
  fi.days_of_supply
FROM dwh.fact_inventory_daily fi
JOIN dwh.dim_date d ON fi.date_key = d.date_key
JOIN dwh.dim_product p ON fi.product_key = p.product_key
WHERE p.product_key = 1
ORDER BY d.full_date
LIMIT 15`,
      },
      {
        type: "text",
        content: `### 粒度と集計の関係

粒度が細かいファクトは、より粗い粒度に**集約（ロールアップ）** できますが、逆はできません。

\`\`\`
細かい粒度 --集約--> 粗い粒度
注文x商品  -------> 注文単位 -------> 日単位 -------> 月単位
（元に戻せない）
\`\`\`

したがって、**可能な限り細かい粒度でファクトを保持** し、必要に応じて集約するのが原則です。

### 粒度の落とし穴

#### ファクトレスファクト
計測値を持たないファクトテーブル。イベントの発生自体が重要な場合に使います。

例：「学生が授業に出席した」→ 出席テーブル（計測値は COUNT のみ）

#### 集約と二重カウント
粒度が混在すると、SUM や COUNT で誤った結果が出ます。例えば、注文ヘッダー情報（送料など）を明細行に持たせると、注文あたり複数回カウントされてしまいます。

### まとめ

- **粒度** = ファクトテーブルの1行が何を表すか
- ファクト設計で **最初に** 決めるべきこと
- 原則として **最も細かい粒度** で保持する
- 粒度が混在すると集計が壊れるので要注意
- 粒度の宣言は自然言語で明確に書く`,
      },
    ],
  },
  {
    id: "bridge-table",
    title: "7. ブリッジテーブル",
    subtitle: "多対多の関係をスタースキーマで扱う",
    sections: [
      {
        type: "text",
        content: `## 多対多の問題

スタースキーマでは、ファクトとディメンションの関係は通常 **多対1** です（1つのファクト行は1つのディメンション値を持つ）。

しかし、ビジネスには **多対多** の関係が存在します：

- 1つの患者に複数の診断が付く
- 1つの口座に複数の名義人がいる
- 1つの注文に複数のプロモーションが適用される

### ブリッジテーブルとは

多対多の関係を解決するための中間テーブルです。ファクトとディメンションの間に配置します。`,
      },
      {
        type: "diagram",
        content: `多対多なし (通常のスター型):

  dim_customer <---- fact_sales ----> dim_product
      1:N               N:1

多対多あり (ブリッジテーブル使用):

  dim_promotion <-- bridge_order_promotion --> fact_sales
                    +------------------------+
                    | order_id            FK |
                    | promotion_key       FK |
                    | weight_factor          |  <-- 按分係数
                    +------------------------+

  1つの注文に複数プロモーション -> ブリッジで中間テーブル化`,
      },
      {
        type: "text",
        content: `### ブリッジテーブルの構造

| カラム | 説明 |
|--------|------|
| ファクト側のキー | ファクトテーブルへの参照 |
| ディメンション側のキー | ディメンションテーブルへの参照 |
| 按分係数（weight_factor） | 合計が1になるよう配分（オプション） |

### 按分係数の例

1つの注文に3つのプロモーションが適用された場合：

| order_id | promotion_key | weight_factor |
|----------|---------------|---------------|
| 1001 | 10 | 0.33 |
| 1001 | 20 | 0.33 |
| 1001 | 30 | 0.34 |

売上を按分する場合：\`SUM(f.net_amount * b.weight_factor)\`

### 注意点

- ブリッジテーブルを使うと **JOINの結果行が増える（ファンアウト）**
- 必ず按分係数を使うか、DISTINCT で重複を除去する
- 集計時に二重カウントが発生しないよう注意が必要

### ブリッジテーブルを避ける方法

多対多が発生する前に、ビジネスルールで対処できる場合もあります：

1. **「主」を決める**: 複数プロモーションのうち、最も影響が大きいものだけをファクトに持つ
2. **粒度を変える**: ファクトの粒度を「注文×プロモーション」にする
3. **配列型を使う**: BigQuery の ARRAY 型で1カラムに複数値を持つ（モダンDWH）`,
      },
      {
        type: "exercise",
        title: "演習: 多対多の例を考える",
        description:
          "このDWHでは fact_sales と dim_coupon が 多対1 で設計されています（1注文に1クーポン）。もし複数クーポン適用を許可した場合、どうなるか確認してみましょう。",
        sql: `-- 現在の設計: 1注文 = 1クーポン（多対1）
SELECT
  f.order_id,
  c.coupon_code,
  c.coupon_type,
  c.discount_value,
  f.discount_rate,
  f.net_amount
FROM dwh.fact_sales f
JOIN dwh.dim_coupon c ON f.coupon_key = c.coupon_key
WHERE c.coupon_code != 'N/A'
LIMIT 10`,
        hint: "現在は coupon_key が直接 fact_sales にあるため、多対1です。複数クーポンを許可するなら bridge_order_coupon テーブルが必要になります。",
      },
      {
        type: "text",
        content: `### まとめ

- **ブリッジテーブル** = 多対多の関係を解決する中間テーブル
- 按分係数（weight_factor）で売上などを配分できる
- ファンアウト（行の増加）に注意。二重カウントを防ぐ
- 可能であれば、ビジネスルールや粒度変更で回避することも検討する
- このプロジェクトでは多対1設計で簡略化している`,
      },
    ],
  },
  {
    id: "design-patterns",
    title: "8. 設計パターン集",
    subtitle: "実務でよく使うディメンショナルモデリングのパターン",
    sections: [
      {
        type: "text",
        content: `## 日付ディメンション（Date Dimension）

ほぼすべてのDWHに存在する、最も基本的なコンフォームドディメンションです。

### 特徴
- **事前生成**: 実際のデータに関係なく、カレンダーの全日付を事前に生成する
- **豊富な属性**: 年、月、四半期、曜日、祝日フラグなど
- **時刻とは分離**: 日付と時刻は別ディメンションにする（粒度が異なるため）

### なぜ DATE 型ではなく専用テーブルにするのか？

\`WHERE order_date = '2024-01-15'\` ではなく \`WHERE d.is_holiday = true\` のような分析を可能にするため。BI ツールのフィルタやドリルダウンにも対応しやすくなります。`,
      },
      {
        type: "exercise",
        title: "演習: 日付ディメンションの豊富な属性",
        description:
          "dim_date の構造を確認し、単なる日付値では得られない属性（曜日、四半期、祝日情報など）がどれだけあるか確認しましょう。",
        sql: `-- dim_date のカラムと値を確認
SELECT *
FROM dwh.dim_date
LIMIT 5`,
      },
      {
        type: "exercise",
        title: "演習: 日付ディメンションを活用した分析",
        description:
          "日付ディメンションの属性を使って、曜日別の売上パターンを分析してみましょう。これは DATE 型だけでは難しい分析です。",
        sql: `-- 曜日別の売上パターン
SELECT
  d.day_of_week,
  d.day_name,
  COUNT(DISTINCT f.order_id) AS order_count,
  SUM(f.net_amount) AS total_sales,
  ROUND(AVG(f.net_amount), 0) AS avg_sale_amount
FROM dwh.fact_sales f
JOIN dwh.dim_date d ON f.date_key = d.date_key
GROUP BY 1, 2
ORDER BY 1`,
      },
      {
        type: "text",
        content: `## ジャンクディメンション（Junk Dimension）

低カーディナリティ（値の種類が少ない）のフラグやインジケータを、1つのディメンションにまとめるパターンです。

### 例

| is_weekend | is_holiday | is_campaign | → junk_key |
|------------|-----------|-------------|------------|
| true | false | false | 1 |
| true | false | true | 2 |
| false | true | false | 3 |
| ... | ... | ... | ... |

すべての組み合わせをあらかじめ生成しておきます。フラグが5つ（各2値）なら 2^5 = 32 行で済みます。

### メリット
- ファクトテーブルの FK カラムを減らせる
- 個別のフラグカラムをファクトに持つより整理される

## ロールプレイングディメンション（Role-Playing Dimension）

1つのディメンションテーブルを、異なる役割で複数回参照するパターンです。

最も典型的なのは **日付ディメンション** です：`,
      },
      {
        type: "diagram",
        content: `ロールプレイングディメンション (日付の例):

                    +-----------------+
  order_date_key -->|                 |
                    |    dim_date     |
  ship_date_key  -->|                 |<-- 同じテーブルを
                    |  date_key  PK   |    異なるFKで参照
  deliver_date   -->|  year, month    |
    _key            |  day_of_week    |
                    +-----------------+

  fact_order:
    order_date_key   FK -> dim_date  (注文日)
    ship_date_key    FK -> dim_date  (出荷日)
    deliver_date_key FK -> dim_date  (配達日)`,
      },
      {
        type: "text",
        content: `## ミニディメンション

大規模なディメンション（数百万行の顧客など）から、頻繁に変化する属性を切り出すパターンです。

### 問題
顧客テーブルが1000万行あり、SCD Type 2 で管理している場合、属性が変わるたびに新しい行が追加され、テーブルが肥大化します。

### 解決策
よく変わる属性（年収帯、年齢帯、アクティビティレベルなど）を別テーブルに切り出します。

\`\`\`
dim_customer:          dim_customer_profile:
  customer_key           profile_key
  name                   age_band       (20代, 30代, ...)
  email                  income_band    (300万〜, 500万〜, ...)
  registration_date      activity_level (高, 中, 低)
\`\`\`

fact_sales は両方のキーを持ちます。プロファイルの変更は dim_customer_profile の行追加で済み、dim_customer は安定します。

## 集約テーブル（Aggregate Table）

パフォーマンス最適化のために、事前集計したファクトテーブルを用意するパターンです。

\`\`\`
fact_sales          → fact_sales_monthly_summary
(注文×商品 粒度)      (商品×月 粒度)
\`\`\`

### 現代の DWH では

BigQuery や Snowflake などのクラウドDWHは大規模データの集計が高速なため、集約テーブルの必要性は低下しています。ただし、ダッシュボードの応答速度改善などの目的で依然として有用です。`,
      },
      {
        type: "exercise",
        title: "演習: 設計パターンを見つける",
        description:
          "このDWHで使われている設計パターンを確認しましょう。dim_time は dim_date と分離された「時刻ディメンション」です。dim_order_status は低カーディナリティのディメンション（ジャンクディメンション的）です。",
        sql: `-- dim_time: 日付とは分離された時刻ディメンション
SELECT *
FROM dwh.dim_time
LIMIT 10`,
      },
      {
        type: "exercise",
        title: "演習: ディメンションの統計を確認する",
        description:
          "各ディメンションのカーディナリティ（値の種類数）を確認し、ジャンクディメンション候補がないか考えてみましょう。",
        sql: `-- 各ディメンションの行数を比較
SELECT 'dim_date' AS dimension, COUNT(*) AS row_count FROM dwh.dim_date
UNION ALL
SELECT 'dim_time', COUNT(*) FROM dwh.dim_time
UNION ALL
SELECT 'dim_customer', COUNT(*) FROM dwh.dim_customer
UNION ALL
SELECT 'dim_product', COUNT(*) FROM dwh.dim_product
UNION ALL
SELECT 'dim_order_status', COUNT(*) FROM dwh.dim_order_status
UNION ALL
SELECT 'dim_coupon', COUNT(*) FROM dwh.dim_coupon
ORDER BY row_count`,
        hint: "dim_order_status の行数が極端に少なければ、ジャンクディメンション的な役割です。低カーディナリティの属性をまとめたテーブルです。",
      },
      {
        type: "text",
        content: `### まとめ: 主要な設計パターン

| パターン | 用途 | 例 |
|---------|------|-----|
| **日付ディメンション** | 時間軸の分析 | dim_date |
| **時刻ディメンション** | 時間帯分析 | dim_time |
| **ジャンクディメンション** | フラグ・インジケータの整理 | dim_order_status |
| **ロールプレイング** | 1テーブルを複数役割で参照 | 注文日/出荷日/配達日 |
| **ミニディメンション** | 頻繁に変わる属性の分離 | 顧客プロファイル |
| **ブリッジテーブル** | 多対多の解決 | 注文×プロモーション |
| **集約テーブル** | パフォーマンス最適化 | 月次サマリー |

これらのパターンを組み合わせて、ビジネス要件に合ったスタースキーマを設計します。

### おわりに

ディメンショナルモデリングの基礎を学びました。実際の設計では、ビジネスプロセスの理解が最も重要です。技術的なパターンはツールに過ぎず、「何を計測し、どの切り口で分析したいか」というビジネスの問いが出発点になります。

Query タブで自由にSQLを実行し、このDWHのスタースキーマを探索してみてください。`,
      },
    ],
  },
];
