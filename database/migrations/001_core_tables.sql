-- ============================================================
-- Migration 001: Core Tables
-- exchanges, instruments, funding_rates
-- ============================================================

-- Exchange configurations
CREATE TABLE IF NOT EXISTS exchanges (
    id              SERIAL PRIMARY KEY,
    name            VARCHAR(50) NOT NULL,            -- 'okx', 'binance'
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(name)
);

-- Tradeable instruments / pairs
CREATE TABLE IF NOT EXISTS instruments (
    id              SERIAL PRIMARY KEY,
    instrument_id   VARCHAR(100) NOT NULL UNIQUE,    -- standardized: '{exchange}_{type}_{base}_{quote}' e.g. 'okx_perpetual_BTC_USDT'
    exchange_id     INT NOT NULL REFERENCES exchanges(id),
    symbol          VARCHAR(50) NOT NULL,            -- exchange native symbol: 'BTC-USDT', 'BTC-USDT-SWAP'
    type VARCHAR(20) NOT NULL,                       -- 'SPOT', 'PERP', 'FUTURES', 'OPTION'
    base_currency   VARCHAR(20) NOT NULL,            -- 'BTC'
    quote_currency  VARCHAR(20) NOT NULL,            -- 'USDT'
    settle_currency VARCHAR(20) NOT NULL,            -- 'USDT'
    contract_size   DECIMAL,                         -- contract multiplier for derivatives (e.g. 0.01 BTC per contract)
    multiplier      int not null default 1,
    min_size        DECIMAL,                         -- minimum order size
    is_active       BOOLEAN DEFAULT TRUE,            -- whether tradeable
    listing_time    TIMESTAMPTZ,                     -- when listed on exchange
    metadata        JSONB DEFAULT '{}',              -- extra info (margin mode, max leverage, etc.)
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(exchange_id, symbol, type)
);

-- Funding rate historical data
CREATE TABLE IF NOT EXISTS funding_rates (
    id              SERIAL PRIMARY KEY,
    exchange_id     INT NOT NULL REFERENCES exchanges(id),
    instrument_id   INT REFERENCES instruments(id),  -- link to the perpetual instrument
    funding_rate    DECIMAL NOT NULL,                -- e.g. 0.0001 = 0.01%
    predicted_rate  DECIMAL,                         -- next predicted rate (if available)
    funding_time    TIMESTAMPTZ NOT NULL,            -- settlement timestamp
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(instrument_id, funding_time)
);

-- ============================================================
-- INDEXES
-- ============================================================

-- Instruments
CREATE INDEX idx_instruments_exchange ON instruments(exchange_id);
CREATE INDEX idx_instruments_type ON instruments(type);
CREATE INDEX idx_instruments_base ON instruments(base_currency);
CREATE INDEX idx_instruments_active ON instruments(exchange_id, is_active);
-- instrument_id already has UNIQUE constraint which creates an index

-- Funding rates
CREATE INDEX idx_funding_rates_instrument_time ON funding_rates(instrument_id, funding_time DESC);
CREATE INDEX idx_funding_rates_exchange ON funding_rates(exchange_id);
CREATE INDEX idx_funding_rates_time ON funding_rates(funding_time DESC);

-- No seed data — exchange records should be inserted manually.
