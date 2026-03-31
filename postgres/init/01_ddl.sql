-- =============================================================
-- EC サイト OLTP スキーマ DDL
-- テーブル作成順は依存関係に従う:
--   customers, categories → products, orders → order_items
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
    created_at    TIMESTAMP     DEFAULT CURRENT_TIMESTAMP
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
    created_at    TIMESTAMP     DEFAULT CURRENT_TIMESTAMP
);

-- 注文ヘッダ
CREATE TABLE orders (
    order_id          SERIAL PRIMARY KEY,
    customer_id       INTEGER       NOT NULL REFERENCES customers(customer_id),
    order_date        DATE          NOT NULL,
    shipping_address  TEXT,
    status            VARCHAR(20)   NOT NULL,
    created_at        TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
    updated_at        TIMESTAMP     DEFAULT CURRENT_TIMESTAMP
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
