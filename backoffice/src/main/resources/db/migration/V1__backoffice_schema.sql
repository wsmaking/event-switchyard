CREATE TABLE IF NOT EXISTS order_meta (
    order_id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS order_filled_total (
    order_id TEXT PRIMARY KEY,
    filled_qty_total BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS positions (
    account_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    net_qty BIGINT NOT NULL,
    avg_price DOUBLE PRECISION,
    PRIMARY KEY (account_id, symbol)
);

CREATE TABLE IF NOT EXISTS balances (
    account_id TEXT NOT NULL,
    currency TEXT NOT NULL,
    amount BIGINT NOT NULL,
    PRIMARY KEY (account_id, currency)
);

CREATE TABLE IF NOT EXISTS realized_pnl (
    account_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    quote_ccy TEXT NOT NULL,
    realized_pnl BIGINT NOT NULL,
    PRIMARY KEY (account_id, symbol, quote_ccy)
);

CREATE TABLE IF NOT EXISTS fills (
    id BIGSERIAL PRIMARY KEY,
    at TIMESTAMPTZ NOT NULL,
    account_id TEXT NOT NULL,
    order_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    filled_qty_delta BIGINT NOT NULL,
    filled_qty_total BIGINT NOT NULL,
    price BIGINT,
    quote_ccy TEXT NOT NULL,
    quote_cash_delta BIGINT NOT NULL,
    fee_quote BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS fills_account_idx ON fills (account_id, id);
CREATE INDEX IF NOT EXISTS fills_order_idx ON fills (order_id, id);
