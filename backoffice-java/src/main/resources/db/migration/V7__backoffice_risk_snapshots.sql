CREATE TABLE IF NOT EXISTS bo_risk_snapshots (
    account_id VARCHAR(128) PRIMARY KEY,
    generated_at BIGINT NOT NULL,
    payload TEXT NOT NULL
);
