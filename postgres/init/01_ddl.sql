-- =============================================================
-- EC サイト OLTP スキーマ DDL
-- テーブル作成順は依存関係に従う:
--   customers, categories → products → coupons, orders → order_items, page_views, inventory_snapshots
-- =============================================================

-- 顧客マスタ
CREATE TABLE customers (
    customer_id   SERIAL PRIMARY KEY,
    first_name    VARCHAR(50)   NOT NULL,
    last_name     VARCHAR(50)   NOT NULL,
    email         VARCHAR(255)  NOT NULL UNIQUE,
    phone         VARCHAR(20),
    city          VARCHAR(100),
    prefecture    VARCHAR(50),
    postal_code   VARCHAR(10),
    created_at    TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
    gender              VARCHAR(10),
    birth_date          DATE,
    registration_channel VARCHAR(30)
);

-- 商品カテゴリ（自己参照による2階層構造）
CREATE TABLE categories (
    category_id   SERIAL PRIMARY KEY,
    name          VARCHAR(100)  NOT NULL,
    parent_id     INTEGER       REFERENCES categories(category_id)
);

-- 商品マスタ
CREATE TABLE products (
    product_id    SERIAL PRIMARY KEY,
    product_name  VARCHAR(200)  NOT NULL,
    category_id   INTEGER       NOT NULL REFERENCES categories(category_id),
    unit_price    NUMERIC(10,2) NOT NULL,
    description   TEXT,
    created_at    TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
    cost_price    NUMERIC(10,2),
    weight_gram   INTEGER,
    is_active     BOOLEAN       DEFAULT TRUE
);

-- クーポンマスタ
CREATE TABLE coupons (
    coupon_id        SERIAL PRIMARY KEY,
    coupon_code      VARCHAR(50)   NOT NULL UNIQUE,
    coupon_type      VARCHAR(20)   NOT NULL,
    discount_value   NUMERIC(10,2) NOT NULL,
    min_order_amount NUMERIC(10,2),
    valid_from       DATE          NOT NULL,
    valid_to         DATE          NOT NULL,
    campaign_name    VARCHAR(100)
);

-- 注文ヘッダ
CREATE TABLE orders (
    order_id          SERIAL PRIMARY KEY,
    customer_id       INTEGER       NOT NULL REFERENCES customers(customer_id),
    order_date        DATE          NOT NULL,
    shipping_address  TEXT,
    status            VARCHAR(20)   NOT NULL,
    created_at        TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
    updated_at        TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
    order_timestamp   TIMESTAMP     NOT NULL,
    payment_method    VARCHAR(30),
    device_type       VARCHAR(20),
    coupon_code       VARCHAR(50)
);

-- 注文明細
CREATE TABLE order_items (
    order_item_id SERIAL PRIMARY KEY,
    order_id      INTEGER       NOT NULL REFERENCES orders(order_id),
    product_id    INTEGER       NOT NULL REFERENCES products(product_id),
    quantity      INTEGER       NOT NULL,
    unit_price    NUMERIC(10,2) NOT NULL,
    discount      NUMERIC(5,2)  DEFAULT 0
);

-- ページビュー（アクセスログ）
CREATE TABLE page_views (
    page_view_id    SERIAL PRIMARY KEY,
    customer_id     INTEGER       REFERENCES customers(customer_id),
    session_id      VARCHAR(50)   NOT NULL,
    page_type       VARCHAR(30)   NOT NULL,
    product_id      INTEGER       REFERENCES products(product_id),
    category_id     INTEGER       REFERENCES categories(category_id),
    referrer_type   VARCHAR(30),
    view_timestamp  TIMESTAMP     NOT NULL,
    duration_seconds INTEGER
);

-- 日次在庫スナップショット
CREATE TABLE inventory_snapshots (
    snapshot_id      SERIAL PRIMARY KEY,
    product_id       INTEGER       NOT NULL REFERENCES products(product_id),
    snapshot_date    DATE          NOT NULL,
    quantity_on_hand INTEGER       NOT NULL,
    quantity_reserved INTEGER      NOT NULL DEFAULT 0,
    reorder_point    INTEGER       NOT NULL,
    lead_time_days   INTEGER       NOT NULL,
    UNIQUE (product_id, snapshot_date)
);
