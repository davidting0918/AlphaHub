-- ============================================================
-- Migration 001: Core Tables
-- exchanges, instruments, funding_rates
-- ============================================================

-- Exchange configurations
CREATE TABLE IF NOT EXISTS exchanges (
    id              SERIAL PRIMARY KEY,
    name            VARCHAR(50) NOT NULL,            -- 'okx', 'binance'
    environment     VARCHAR(20) NOT NULL,            -- 'testnet', 'mainnet'
    base_url        VARCHAR(255),                    -- REST API base URL
    ws_url          VARCHAR(255),                    -- WebSocket URL
    fee_structure   JSONB DEFAULT '{}',              -- { "spot_maker": 0.001, "spot_taker": 0.001, "perp_maker": 0.0002, "perp_taker": 0.0005 }
    metadata        JSONB DEFAULT '{}',              -- extra config
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(name, environment)
);

-- Tradeable instruments / pairs
CREATE TABLE IF NOT EXISTS instruments (
    id              SERIAL PRIMARY KEY,
    exchange_id     INT NOT NULL REFERENCES exchanges(id),
    symbol          VARCHAR(50) NOT NULL,            -- exchange native symbol: 'BTC-USDT', 'BTC-USDT-SWAP'
    instrument_type VARCHAR(20) NOT NULL,            -- 'spot', 'perpetual', 'futures', 'option'
    base_currency   VARCHAR(20) NOT NULL,            -- 'BTC'
    quote_currency  VARCHAR(20) NOT NULL,            -- 'USDT'
    contract_size   DECIMAL,                         -- contract multiplier for derivatives (e.g. 0.01 BTC per contract)
    tick_size       DECIMAL,                         -- minimum price increment
    lot_size        DECIMAL,                         -- minimum quantity increment
    min_size        DECIMAL,                         -- minimum order size
    is_active       BOOLEAN DEFAULT TRUE,            -- whether tradeable
    listing_time    TIMESTAMPTZ,                     -- when listed on exchange
    metadata        JSONB DEFAULT '{}',              -- extra info (margin mode, max leverage, etc.)
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(exchange_id, symbol, instrument_type)
);

-- Funding rate historical data
CREATE TABLE IF NOT EXISTS funding_rates (
    id              SERIAL PRIMARY KEY,
    exchange_id     INT NOT NULL REFERENCES exchanges(id),
    instrument_id   INT REFERENCES instruments(id),  -- link to the perpetual instrument
    symbol          VARCHAR(50) NOT NULL,            -- 'BTC-USDT-SWAP'
    funding_rate    DECIMAL NOT NULL,                -- e.g. 0.0001 = 0.01%
    predicted_rate  DECIMAL,                         -- next predicted rate (if available)
    funding_time    TIMESTAMPTZ NOT NULL,            -- settlement timestamp
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(exchange_id, symbol, funding_time)
);

-- ============================================================
-- INDEXES
-- ============================================================

-- Instruments
CREATE INDEX idx_instruments_exchange ON instruments(exchange_id);
CREATE INDEX idx_instruments_type ON instruments(instrument_type);
CREATE INDEX idx_instruments_base ON instruments(base_currency);
CREATE INDEX idx_instruments_active ON instruments(exchange_id, is_active);

-- Funding rates
CREATE INDEX idx_funding_rates_symbol_time ON funding_rates(symbol, funding_time DESC);
CREATE INDEX idx_funding_rates_exchange ON funding_rates(exchange_id, symbol);
CREATE INDEX idx_funding_rates_instrument ON funding_rates(instrument_id, funding_time DESC);
CREATE INDEX idx_funding_rates_time ON funding_rates(funding_time DESC);

-- ============================================================
-- SEED DATA
-- ============================================================

INSERT INTO exchanges (name, environment, base_url, ws_url, fee_structure) VALUES
    ('okx', 'testnet',
     'https://www.okx.com',
     'wss://ws.okx.com:8443/ws/v5',
     '{"spot_maker": 0.001, "spot_taker": 0.001, "perp_maker": 0.0002, "perp_taker": 0.0005}'
    ),
    ('okx', 'mainnet',
     'https://www.okx.com',
     'wss://ws.okx.com:8443/ws/v5',
     '{"spot_maker": 0.001, "spot_taker": 0.001, "perp_maker": 0.0002, "perp_taker": 0.0005}'
    ),
    ('binance', 'mainnet',
     'https://api.binance.com',
     'wss://stream.binance.com:9443/ws',
     '{"spot_maker": 0.001, "spot_taker": 0.001, "perp_maker": 0.0002, "perp_taker": 0.0005}'
    ),
    ('binance', 'testnet',
     'https://testnet.binance.vision',
     'wss://testnet.binance.vision/ws',
     '{"spot_maker": 0.001, "spot_taker": 0.001, "perp_maker": 0.0002, "perp_taker": 0.0005}'
    )
ON CONFLICT (name, environment) DO NOTHING;
