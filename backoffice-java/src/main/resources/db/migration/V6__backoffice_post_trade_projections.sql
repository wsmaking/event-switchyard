CREATE TABLE IF NOT EXISTS bo_settlement_projections (
    order_id VARCHAR(128) PRIMARY KEY,
    account_id VARCHAR(128) NOT NULL,
    symbol VARCHAR(64) NOT NULL,
    generated_at BIGINT NOT NULL,
    payload TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS bo_settlement_projections_account_idx
    ON bo_settlement_projections (account_id, generated_at DESC);

CREATE TABLE IF NOT EXISTS bo_statement_projections (
    order_id VARCHAR(128) PRIMARY KEY,
    account_id VARCHAR(128) NOT NULL,
    symbol VARCHAR(64) NOT NULL,
    generated_at BIGINT NOT NULL,
    payload TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS bo_statement_projections_account_idx
    ON bo_statement_projections (account_id, generated_at DESC);
