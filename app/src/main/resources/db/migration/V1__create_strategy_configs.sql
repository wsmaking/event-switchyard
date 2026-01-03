CREATE SCHEMA IF NOT EXISTS app;

CREATE TABLE IF NOT EXISTS app.strategy_configs (
  account_id TEXT PRIMARY KEY,
  enabled BOOLEAN NOT NULL,
  symbols TEXT NOT NULL,
  tick_ms BIGINT NOT NULL,
  max_orders_per_min INTEGER NOT NULL,
  cooldown_ms BIGINT NOT NULL,
  updated_at_ms BIGINT NOT NULL
);
