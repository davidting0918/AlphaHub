-- ============================================================
-- OPTIONS DATA PIPELINE SCHEMA
-- ============================================================
-- Tables for storing options data from Deribit, Binance, and OKX
-- ============================================================

-- Options instruments from all exchanges
CREATE TABLE IF NOT EXISTS options_instruments (
    id SERIAL PRIMARY KEY,
    instrument_id VARCHAR(100) NOT NULL UNIQUE,
    exchange VARCHAR(20) NOT NULL, -- DERIBIT, BINANCE, OKX
    underlying VARCHAR(20) NOT NULL, -- BTC, ETH
    strike DECIMAL(20,2) NOT NULL,
    expiry TIMESTAMPTZ NOT NULL,
    option_type VARCHAR(1) NOT NULL, -- C or P
    settlement VARCHAR(10), -- cash, physical
    contract_size DECIMAL(20,8),
    tick_size DECIMAL(20,8),
    is_active BOOLEAN DEFAULT TRUE,
    raw_data JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Historical option tickers/snapshots
CREATE TABLE IF NOT EXISTS options_tickers (
    id SERIAL PRIMARY KEY,
    instrument_id VARCHAR(100) NOT NULL,
    exchange VARCHAR(20) NOT NULL,
    mark_price DECIMAL(20,8),
    bid_price DECIMAL(20,8),
    ask_price DECIMAL(20,8),
    last_price DECIMAL(20,8),
    iv DECIMAL(10,4), -- implied volatility (decimal, e.g. 0.65 = 65%)
    delta DECIMAL(10,6),
    gamma DECIMAL(10,8),
    theta DECIMAL(10,6),
    vega DECIMAL(10,6),
    rho DECIMAL(10,6),
    volume_24h DECIMAL(20,8),
    open_interest DECIMAL(20,8),
    underlying_price DECIMAL(20,8),
    timestamp TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(instrument_id, timestamp)
);

-- Aggregated volatility surface
CREATE TABLE IF NOT EXISTS volatility_surface (
    id SERIAL PRIMARY KEY,
    underlying VARCHAR(20) NOT NULL,
    exchange VARCHAR(20) NOT NULL,
    expiry TIMESTAMPTZ NOT NULL,
    strike DECIMAL(20,2) NOT NULL,
    option_type VARCHAR(1) NOT NULL,
    iv DECIMAL(10,4),
    delta DECIMAL(10,6),
    underlying_price DECIMAL(20,8),
    timestamp TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(underlying, exchange, expiry, strike, option_type, timestamp)
);

-- Historical volatility data (realized volatility)
CREATE TABLE IF NOT EXISTS historical_volatility (
    id SERIAL PRIMARY KEY,
    underlying VARCHAR(20) NOT NULL,
    exchange VARCHAR(20) NOT NULL,
    period_days INT NOT NULL, -- 7, 14, 30, 60, 90
    realized_vol DECIMAL(10,4),
    timestamp TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(underlying, exchange, period_days, timestamp)
);

-- ============================================================
-- INDEXES
-- ============================================================

CREATE INDEX IF NOT EXISTS idx_options_instruments_exchange ON options_instruments(exchange);
CREATE INDEX IF NOT EXISTS idx_options_instruments_underlying ON options_instruments(underlying);
CREATE INDEX IF NOT EXISTS idx_options_instruments_expiry ON options_instruments(expiry);
CREATE INDEX IF NOT EXISTS idx_options_instruments_active ON options_instruments(is_active);

CREATE INDEX IF NOT EXISTS idx_options_tickers_instrument ON options_tickers(instrument_id);
CREATE INDEX IF NOT EXISTS idx_options_tickers_exchange ON options_tickers(exchange);
CREATE INDEX IF NOT EXISTS idx_options_tickers_timestamp ON options_tickers(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_options_tickers_instrument_time ON options_tickers(instrument_id, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_volatility_surface_underlying ON volatility_surface(underlying);
CREATE INDEX IF NOT EXISTS idx_volatility_surface_exchange ON volatility_surface(exchange);
CREATE INDEX IF NOT EXISTS idx_volatility_surface_timestamp ON volatility_surface(timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_historical_volatility_underlying ON historical_volatility(underlying);
CREATE INDEX IF NOT EXISTS idx_historical_volatility_timestamp ON historical_volatility(timestamp DESC);
