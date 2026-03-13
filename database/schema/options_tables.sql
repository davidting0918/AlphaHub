-- =====================================================
-- Options Tables — Schema Extension for AlphaHub
-- =====================================================
-- Tables: options_instruments, options_tickers, volatility_surface
-- Run against existing AlphaHub database.

-- 1. Options Instruments — instrument metadata
CREATE TABLE IF NOT EXISTS options_instruments (
    id              BIGSERIAL PRIMARY KEY,
    instrument_id   TEXT        NOT NULL UNIQUE,   -- e.g. "DERIBIT_OPT_BTC-28MAR25-100000-C"
    exchange_id     INTEGER     NOT NULL,
    symbol          TEXT        NOT NULL,           -- exchange-native symbol, e.g. "BTC-28MAR25-100000-C"
    underlying      TEXT        NOT NULL,           -- "BTC" or "ETH"
    quote_currency  TEXT        NOT NULL DEFAULT 'USD',
    strike          DOUBLE PRECISION NOT NULL,
    expiry          TIMESTAMPTZ NOT NULL,
    option_type     TEXT        NOT NULL CHECK (option_type IN ('C', 'P')),  -- Call / Put
    settlement      TEXT        NOT NULL DEFAULT 'cash',  -- cash / physical
    contract_size   DOUBLE PRECISION DEFAULT 1.0,
    min_trade_amount DOUBLE PRECISION DEFAULT 0.1,
    tick_size       DOUBLE PRECISION DEFAULT 0.0005,
    is_active       BOOLEAN     NOT NULL DEFAULT TRUE,
    creation_time   TIMESTAMPTZ,
    expiration_time TIMESTAMPTZ,                   -- alias for expiry used by some exchanges
    metadata        JSONB       DEFAULT '{}',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_options_instruments_underlying
    ON options_instruments (underlying);
CREATE INDEX IF NOT EXISTS idx_options_instruments_expiry
    ON options_instruments (expiry);
CREATE INDEX IF NOT EXISTS idx_options_instruments_strike
    ON options_instruments (underlying, expiry, strike);
CREATE INDEX IF NOT EXISTS idx_options_instruments_active
    ON options_instruments (is_active) WHERE is_active = TRUE;


-- 2. Options Tickers — historical options ticker snapshots
CREATE TABLE IF NOT EXISTS options_tickers (
    id              BIGSERIAL PRIMARY KEY,
    instrument_id   TEXT        NOT NULL,           -- FK → options_instruments.instrument_id
    exchange_id     INTEGER     NOT NULL,
    underlying      TEXT        NOT NULL,
    -- Price data
    mark_price      DOUBLE PRECISION,
    last_price      DOUBLE PRECISION,
    bid_price       DOUBLE PRECISION,
    ask_price       DOUBLE PRECISION,
    -- Greeks
    delta           DOUBLE PRECISION,
    gamma           DOUBLE PRECISION,
    theta           DOUBLE PRECISION,
    vega            DOUBLE PRECISION,
    rho             DOUBLE PRECISION,
    -- Volatility
    iv              DOUBLE PRECISION,               -- implied volatility (annualised, decimal)
    -- Volume & OI
    volume_24h      DOUBLE PRECISION DEFAULT 0,
    open_interest   DOUBLE PRECISION DEFAULT 0,
    -- Underlying reference
    underlying_price DOUBLE PRECISION,
    underlying_index TEXT,                           -- e.g. "btc_usd" index name
    -- Timestamp
    timestamp       TIMESTAMPTZ NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE (instrument_id, timestamp)
);

CREATE INDEX IF NOT EXISTS idx_options_tickers_underlying_ts
    ON options_tickers (underlying, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_options_tickers_instrument_ts
    ON options_tickers (instrument_id, timestamp DESC);


-- 3. Volatility Surface — IV surface snapshots
CREATE TABLE IF NOT EXISTS volatility_surface (
    id              BIGSERIAL PRIMARY KEY,
    underlying      TEXT        NOT NULL,           -- "BTC" or "ETH"
    expiry          TIMESTAMPTZ NOT NULL,
    strike          DOUBLE PRECISION NOT NULL,
    option_type     TEXT        NOT NULL CHECK (option_type IN ('C', 'P')),
    iv              DOUBLE PRECISION NOT NULL,       -- implied vol (annualised, decimal)
    delta           DOUBLE PRECISION,
    underlying_price DOUBLE PRECISION,
    timestamp       TIMESTAMPTZ NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE (underlying, expiry, strike, option_type, timestamp)
);

CREATE INDEX IF NOT EXISTS idx_vol_surface_underlying_ts
    ON volatility_surface (underlying, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_vol_surface_expiry
    ON volatility_surface (underlying, expiry, timestamp DESC);
