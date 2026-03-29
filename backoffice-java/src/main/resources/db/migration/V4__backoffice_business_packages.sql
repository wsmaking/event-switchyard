CREATE TABLE IF NOT EXISTS bo_execution_packages (
    order_id VARCHAR(128) PRIMARY KEY,
    account_id VARCHAR(128) NOT NULL,
    symbol VARCHAR(64) NOT NULL,
    generated_at BIGINT NOT NULL,
    payload TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS bo_execution_packages_account_idx
    ON bo_execution_packages (account_id, generated_at DESC);

CREATE TABLE IF NOT EXISTS bo_post_trade_packages (
    order_id VARCHAR(128) PRIMARY KEY,
    account_id VARCHAR(128) NOT NULL,
    symbol VARCHAR(64) NOT NULL,
    generated_at BIGINT NOT NULL,
    payload TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS bo_post_trade_packages_account_idx
    ON bo_post_trade_packages (account_id, generated_at DESC);
