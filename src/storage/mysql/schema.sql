CREATE TABLE users (
    id VARCHAR(64) PRIMARY KEY,
    username VARCHAR(128) NOT NULL UNIQUE,
    role VARCHAR(32) NOT NULL,
    password_hash VARBINARY(255) NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE products (
    id VARCHAR(64) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    price_cents BIGINT NOT NULL,
    on_sale BOOLEAN NOT NULL DEFAULT FALSE,
    version BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE inventory (
    sku_id VARCHAR(64) PRIMARY KEY,
    available BIGINT NOT NULL,
    reserved BIGINT NOT NULL DEFAULT 0,
    sold BIGINT NOT NULL DEFAULT 0,
    version BIGINT NOT NULL DEFAULT 0,
    CHECK (available >= 0 AND reserved >= 0 AND sold >= 0)
);

CREATE TABLE orders (
    id VARCHAR(64) PRIMARY KEY,
    user_id VARCHAR(64) NOT NULL,
    idempotency_key VARCHAR(128) NOT NULL UNIQUE,
    state VARCHAR(32) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE order_items (
    order_id VARCHAR(64) NOT NULL,
    sku_id VARCHAR(64) NOT NULL,
    quantity BIGINT NOT NULL,
    PRIMARY KEY (order_id, sku_id)
);

CREATE TABLE outbox_events (
    event_id VARCHAR(128) PRIMARY KEY,
    topic VARCHAR(128) NOT NULL,
    event_key VARCHAR(128) NOT NULL,
    payload BLOB NOT NULL,
    published BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_outbox_pending ON outbox_events (published, created_at, event_id);
