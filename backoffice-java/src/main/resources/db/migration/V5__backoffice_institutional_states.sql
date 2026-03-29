CREATE TABLE IF NOT EXISTS bo_parent_execution_states (
    order_id VARCHAR(128) PRIMARY KEY,
    account_id VARCHAR(128) NOT NULL,
    symbol VARCHAR(64) NOT NULL,
    generated_at BIGINT NOT NULL,
    payload TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS bo_parent_execution_states_account_idx
    ON bo_parent_execution_states (account_id, generated_at DESC);

CREATE TABLE IF NOT EXISTS bo_allocation_states (
    order_id VARCHAR(128) PRIMARY KEY,
    account_id VARCHAR(128) NOT NULL,
    symbol VARCHAR(64) NOT NULL,
    generated_at BIGINT NOT NULL,
    payload TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS bo_allocation_states_account_idx
    ON bo_allocation_states (account_id, generated_at DESC);
