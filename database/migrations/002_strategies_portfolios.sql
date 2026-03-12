-- ============================================================
-- Migration 002: Strategies & Portfolios
-- ============================================================

-- Strategies
CREATE TABLE IF NOT EXISTS strategies (
    id              SERIAL PRIMARY KEY,
    name            VARCHAR(100) NOT NULL UNIQUE,    -- 'funding_rate_arb'
    display_name    VARCHAR(200),                    -- 'Funding Rate Arbitrage'
    strategy_type   VARCHAR(50) NOT NULL,            -- 'arbitrage', 'momentum', 'options'
    description     TEXT,
    default_config  JSONB DEFAULT '{}',              -- default strategy parameters
    status          VARCHAR(20) DEFAULT 'active',    -- 'active', 'paused', 'stopped'
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

-- Portfolios (child of Strategy — one strategy can have multiple portfolios)
CREATE TABLE IF NOT EXISTS portfolios (
    id              SERIAL PRIMARY KEY,
    name            VARCHAR(100) NOT NULL UNIQUE,    -- 'okx_funding_arb_01'
    strategy_id     INT NOT NULL REFERENCES strategies(id),
    exchange_id     INT NOT NULL REFERENCES exchanges(id),
    initial_capital DECIMAL NOT NULL DEFAULT 0,      -- starting capital
    currency        VARCHAR(20) DEFAULT 'USDT',      -- denomination currency
    status          VARCHAR(20) DEFAULT 'active',    -- 'active', 'paused', 'closed'
    config          JSONB DEFAULT '{}',              -- portfolio-specific params
    notes           TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_strategies_type ON strategies(strategy_type);
CREATE INDEX IF NOT EXISTS idx_strategies_status ON strategies(status);
CREATE INDEX IF NOT EXISTS idx_portfolios_strategy ON portfolios(strategy_id);
CREATE INDEX IF NOT EXISTS idx_portfolios_exchange ON portfolios(exchange_id);
CREATE INDEX IF NOT EXISTS idx_portfolios_status ON portfolios(status);
