-- ============================================================
-- AlphaHub Database Schema
-- Shared across all trading strategies
-- ============================================================

-- Enable UUID generation
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================================
-- CORE TABLES (shared by all strategies)
-- ============================================================

-- Exchange configurations
CREATE TABLE exchanges (
    id              SERIAL PRIMARY KEY,
    name            VARCHAR(50) NOT NULL,           -- 'okx', 'binance'
    environment     VARCHAR(20) NOT NULL,           -- 'testnet', 'mainnet'
    base_url        VARCHAR(255),                   -- API base URL
    metadata        JSONB DEFAULT '{}',             -- extra config
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(name, environment)
);

-- Strategy registry
CREATE TABLE strategies (
    id              SERIAL PRIMARY KEY,
    name            VARCHAR(100) NOT NULL UNIQUE,   -- 'funding_rate_arb'
    display_name    VARCHAR(200),                   -- 'Funding Rate Arbitrage'
    strategy_type   VARCHAR(50) NOT NULL,           -- 'arbitrage', 'momentum', 'options'
    description     TEXT,
    default_config  JSONB DEFAULT '{}',             -- default strategy parameters
    status          VARCHAR(20) DEFAULT 'active',   -- 'active', 'paused', 'stopped'
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

-- Portfolios (one strategy can have multiple portfolios / accounts)
-- Portfolio is a CHILD of Strategy
CREATE TABLE portfolios (
    id              SERIAL PRIMARY KEY,
    strategy_id     INT NOT NULL REFERENCES strategies(id),
    exchange_id     INT NOT NULL REFERENCES exchanges(id),
    name            VARCHAR(100) NOT NULL,          -- 'OKX Testnet - Account A'
    initial_capital DECIMAL NOT NULL,               -- starting capital
    current_capital DECIMAL,                        -- latest equity
    currency        VARCHAR(20) DEFAULT 'USDT',     -- denomination currency
    status          VARCHAR(20) DEFAULT 'active',   -- 'active', 'paused', 'closed'
    config          JSONB DEFAULT '{}',             -- portfolio-specific params (max exposure, etc.)
    notes           TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

-- Tradeable instruments / pairs
CREATE TABLE instruments (
    id              SERIAL PRIMARY KEY,
    exchange_id     INT NOT NULL REFERENCES exchanges(id),
    symbol          VARCHAR(50) NOT NULL,           -- 'BTC-USDT'
    instrument_type VARCHAR(20) NOT NULL,           -- 'spot', 'perpetual', 'futures', 'option'
    base_currency   VARCHAR(20),                    -- 'BTC'
    quote_currency  VARCHAR(20),                    -- 'USDT'
    contract_size   DECIMAL,                        -- for derivatives
    tick_size       DECIMAL,                        -- minimum price increment
    lot_size        DECIMAL,                        -- minimum quantity increment
    metadata        JSONB DEFAULT '{}',
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(exchange_id, symbol, instrument_type)
);

-- ============================================================
-- EXECUTION TABLES
-- ============================================================

-- Order records (every order placed)
CREATE TABLE orders (
    id                  SERIAL PRIMARY KEY,
    portfolio_id        INT NOT NULL REFERENCES portfolios(id),
    instrument_id       INT REFERENCES instruments(id),
    position_group_id   UUID DEFAULT uuid_generate_v4(),  -- groups paired orders (e.g. spot+perp)
    exchange_order_id   VARCHAR(100),               -- order ID from exchange
    side                VARCHAR(10) NOT NULL,        -- 'buy', 'sell'
    order_type          VARCHAR(20) DEFAULT 'market',-- 'market', 'limit', 'stop'
    quantity            DECIMAL NOT NULL,
    price               DECIMAL,                     -- limit price (NULL for market)
    avg_fill_price      DECIMAL,                     -- actual fill price
    filled_quantity     DECIMAL DEFAULT 0,
    fee                 DECIMAL DEFAULT 0,
    fee_currency        VARCHAR(20),
    status              VARCHAR(20) DEFAULT 'pending', -- 'pending', 'open', 'filled', 'partial', 'cancelled', 'failed'
    notes               TEXT,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    filled_at           TIMESTAMPTZ
);

-- Positions (current & historical)
CREATE TABLE positions (
    id                  SERIAL PRIMARY KEY,
    portfolio_id        INT NOT NULL REFERENCES portfolios(id),
    instrument_id       INT REFERENCES instruments(id),
    position_group_id   UUID,                        -- pairs spot+perp positions together
    side                VARCHAR(10) NOT NULL,         -- 'long', 'short'
    quantity            DECIMAL NOT NULL,
    entry_price         DECIMAL NOT NULL,
    current_price       DECIMAL,
    unrealized_pnl      DECIMAL DEFAULT 0,
    realized_pnl        DECIMAL DEFAULT 0,
    status              VARCHAR(20) DEFAULT 'open',   -- 'open', 'closed', 'liquidated'
    opened_at           TIMESTAMPTZ DEFAULT NOW(),
    closed_at           TIMESTAMPTZ,
    metadata            JSONB DEFAULT '{}'            -- strategy-specific data
);

-- ============================================================
-- PNL & BALANCE TRACKING
-- ============================================================

-- PnL snapshots (periodic snapshots for tracking performance)
CREATE TABLE pnl_snapshots (
    id                  SERIAL PRIMARY KEY,
    portfolio_id        INT NOT NULL REFERENCES portfolios(id),
    position_group_id   UUID,                        -- NULL = portfolio-level snapshot
    total_pnl           DECIMAL DEFAULT 0,           -- realized + unrealized
    realized_pnl        DECIMAL DEFAULT 0,
    unrealized_pnl      DECIMAL DEFAULT 0,
    fees_paid           DECIMAL DEFAULT 0,
    funding_earned      DECIMAL DEFAULT 0,           -- for funding strategies
    net_pnl             DECIMAL DEFAULT 0,           -- total - fees + funding
    equity              DECIMAL,                     -- portfolio equity at snapshot time
    snapshot_at         TIMESTAMPTZ DEFAULT NOW()
);

-- Balance snapshots (account balance tracking)
CREATE TABLE balance_snapshots (
    id                  SERIAL PRIMARY KEY,
    portfolio_id        INT NOT NULL REFERENCES portfolios(id),
    currency            VARCHAR(20) NOT NULL,
    available           DECIMAL DEFAULT 0,
    frozen              DECIMAL DEFAULT 0,           -- in open orders / margin
    total               DECIMAL DEFAULT 0,
    snapshot_at         TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- LOGGING
-- ============================================================

-- Strategy logs (signals, decisions, errors)
CREATE TABLE strategy_logs (
    id              SERIAL PRIMARY KEY,
    portfolio_id    INT REFERENCES portfolios(id),   -- NULL = strategy-level log
    strategy_id     INT REFERENCES strategies(id),
    log_type        VARCHAR(20) NOT NULL,            -- 'signal', 'decision', 'execution', 'error', 'info'
    message         TEXT,
    data            JSONB DEFAULT '{}',              -- structured log data
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- STRATEGY-SPECIFIC: FUNDING RATE ARBITRAGE
-- ============================================================

-- Funding rate historical data (shared data, not per-portfolio)
CREATE TABLE funding_rates (
    id              SERIAL PRIMARY KEY,
    exchange_id     INT NOT NULL REFERENCES exchanges(id),
    symbol          VARCHAR(50) NOT NULL,            -- 'BTC-USDT-SWAP'
    funding_rate    DECIMAL NOT NULL,
    predicted_rate  DECIMAL,                         -- next predicted rate
    funding_time    TIMESTAMPTZ NOT NULL,            -- settlement time
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(exchange_id, symbol, funding_time)
);

-- Funding payments received/paid (per portfolio)
CREATE TABLE funding_payments (
    id              SERIAL PRIMARY KEY,
    portfolio_id    INT NOT NULL REFERENCES portfolios(id),
    position_id     INT REFERENCES positions(id),
    symbol          VARCHAR(50),
    funding_rate    DECIMAL,
    payment_amount  DECIMAL,                         -- positive = received, negative = paid
    position_size   DECIMAL,
    payment_time    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- INDEXES
-- ============================================================

-- Portfolio lookups
CREATE INDEX idx_portfolios_strategy ON portfolios(strategy_id);
CREATE INDEX idx_portfolios_status ON portfolios(status);

-- Order lookups
CREATE INDEX idx_orders_portfolio ON orders(portfolio_id);
CREATE INDEX idx_orders_group ON orders(position_group_id);
CREATE INDEX idx_orders_status ON orders(status);
CREATE INDEX idx_orders_created ON orders(created_at);

-- Position lookups
CREATE INDEX idx_positions_portfolio ON positions(portfolio_id);
CREATE INDEX idx_positions_group ON positions(position_group_id);
CREATE INDEX idx_positions_status ON positions(status);

-- PnL lookups
CREATE INDEX idx_pnl_portfolio_time ON pnl_snapshots(portfolio_id, snapshot_at);
CREATE INDEX idx_pnl_group ON pnl_snapshots(position_group_id);

-- Balance lookups
CREATE INDEX idx_balance_portfolio_time ON balance_snapshots(portfolio_id, snapshot_at);

-- Funding rate lookups
CREATE INDEX idx_funding_rates_symbol_time ON funding_rates(symbol, funding_time);
CREATE INDEX idx_funding_rates_exchange ON funding_rates(exchange_id, symbol);

-- Funding payment lookups
CREATE INDEX idx_funding_payments_portfolio ON funding_payments(portfolio_id);
CREATE INDEX idx_funding_payments_position ON funding_payments(position_id);
CREATE INDEX idx_funding_payments_time ON funding_payments(payment_time);

-- Log lookups
CREATE INDEX idx_logs_portfolio ON strategy_logs(portfolio_id, created_at);
CREATE INDEX idx_logs_strategy ON strategy_logs(strategy_id, created_at);
CREATE INDEX idx_logs_type ON strategy_logs(log_type, created_at);

-- ============================================================
-- SEED DATA
-- ============================================================

-- Default exchanges
INSERT INTO exchanges (name, environment, base_url) VALUES
    ('okx', 'testnet', 'https://www.okx.com'),
    ('okx', 'mainnet', 'https://www.okx.com'),
    ('binance', 'mainnet', 'https://api.binance.com');

-- Default strategies
INSERT INTO strategies (name, display_name, strategy_type, description) VALUES
    ('funding_rate_arb', 'Funding Rate Arbitrage', 'arbitrage',
     'Delta-neutral strategy: long spot + short perpetual to collect funding rate payments.');
