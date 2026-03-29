CREATE TABLE IF NOT EXISTS bo_account_hierarchies (
    account_id TEXT PRIMARY KEY,
    generated_at BIGINT NOT NULL,
    payload JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS bo_operator_control_states (
    order_id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    generated_at BIGINT NOT NULL,
    payload JSONB NOT NULL
);
