CREATE TABLE IF NOT EXISTS bo_settlement_exception_workflows (
    order_id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    generated_at BIGINT NOT NULL,
    payload JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS bo_corporate_action_workflows (
    order_id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    generated_at BIGINT NOT NULL,
    payload JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS bo_margin_projections (
    account_id TEXT PRIMARY KEY,
    generated_at BIGINT NOT NULL,
    payload JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS bo_scenario_evaluation_histories (
    account_id TEXT PRIMARY KEY,
    generated_at BIGINT NOT NULL,
    payload JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS bo_backtest_histories (
    account_id TEXT PRIMARY KEY,
    generated_at BIGINT NOT NULL,
    payload JSONB NOT NULL
);
