#!/usr/bin/env python3
"""
Options Data Fetcher — Complete Pipeline

Fetches options data from Deribit, Binance, and OKX public APIs.
Stores everything in PostgreSQL database.
Includes historical data where available.
"""

import os
import sys
import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
import json

import httpx
import asyncpg

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Database URL
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://neondb_owner:npg_mHEynLCfk69D@ep-sweet-unit-a1qsqfu5-pooler.ap-southeast-1.aws.neon.tech/trading?sslmode=require&channel_binding=require"
)


@dataclass
class OptionInstrument:
    """Option instrument definition."""
    instrument_id: str
    exchange: str
    underlying: str
    strike: float
    expiry: datetime
    option_type: str  # C or P
    settlement: Optional[str] = None
    contract_size: Optional[float] = None
    tick_size: Optional[float] = None
    is_active: bool = True
    raw_data: Dict = field(default_factory=dict)


@dataclass
class OptionTicker:
    """Option ticker/snapshot data."""
    instrument_id: str
    exchange: str
    mark_price: Optional[float] = None
    bid_price: Optional[float] = None
    ask_price: Optional[float] = None
    last_price: Optional[float] = None
    iv: Optional[float] = None  # decimal (0.65 = 65%)
    delta: Optional[float] = None
    gamma: Optional[float] = None
    theta: Optional[float] = None
    vega: Optional[float] = None
    rho: Optional[float] = None
    volume_24h: Optional[float] = None
    open_interest: Optional[float] = None
    underlying_price: Optional[float] = None
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


class DatabaseClient:
    """Async PostgreSQL client for options data."""
    
    def __init__(self, url: str):
        self.url = url
        self._pool: Optional[asyncpg.Pool] = None
    
    async def init_pool(self):
        """Initialize connection pool."""
        if self._pool is None:
            self._pool = await asyncpg.create_pool(self.url, min_size=1, max_size=10, command_timeout=60)
            logger.info("Database pool initialized")
    
    async def close(self):
        """Close connection pool."""
        if self._pool:
            await self._pool.close()
            self._pool = None
    
    async def upsert_instrument(self, inst: OptionInstrument) -> bool:
        """Insert or update an option instrument."""
        async with self._pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO options_instruments 
                (instrument_id, exchange, underlying, strike, expiry, option_type, 
                 settlement, contract_size, tick_size, is_active, raw_data)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                ON CONFLICT (instrument_id) 
                DO UPDATE SET is_active = $10, updated_at = NOW()
            """, inst.instrument_id, inst.exchange, inst.underlying, inst.strike,
                inst.expiry, inst.option_type, inst.settlement, inst.contract_size,
                inst.tick_size, inst.is_active, json.dumps(inst.raw_data))
        return True
    
    async def upsert_instruments_batch(self, instruments: List[OptionInstrument]) -> int:
        """Batch upsert instruments."""
        if not instruments:
            return 0
        
        async with self._pool.acquire() as conn:
            # Prepare data
            values = [
                (i.instrument_id, i.exchange, i.underlying, i.strike, i.expiry,
                 i.option_type, i.settlement, i.contract_size, i.tick_size,
                 i.is_active, json.dumps(i.raw_data))
                for i in instruments
            ]
            
            await conn.executemany("""
                INSERT INTO options_instruments 
                (instrument_id, exchange, underlying, strike, expiry, option_type, 
                 settlement, contract_size, tick_size, is_active, raw_data)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                ON CONFLICT (instrument_id) 
                DO UPDATE SET is_active = EXCLUDED.is_active, updated_at = NOW()
            """, values)
        
        return len(instruments)
    
    async def insert_ticker(self, ticker: OptionTicker) -> bool:
        """Insert a ticker snapshot."""
        async with self._pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO options_tickers 
                (instrument_id, exchange, mark_price, bid_price, ask_price, last_price,
                 iv, delta, gamma, theta, vega, rho, volume_24h, open_interest,
                 underlying_price, timestamp)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
                ON CONFLICT (instrument_id, timestamp) DO NOTHING
            """, ticker.instrument_id, ticker.exchange, ticker.mark_price,
                ticker.bid_price, ticker.ask_price, ticker.last_price,
                ticker.iv, ticker.delta, ticker.gamma, ticker.theta, ticker.vega,
                ticker.rho, ticker.volume_24h, ticker.open_interest,
                ticker.underlying_price, ticker.timestamp)
        return True
    
    async def insert_tickers_batch(self, tickers: List[OptionTicker]) -> int:
        """Batch insert tickers."""
        if not tickers:
            return 0
        
        async with self._pool.acquire() as conn:
            values = [
                (t.instrument_id, t.exchange, t.mark_price, t.bid_price, t.ask_price,
                 t.last_price, t.iv, t.delta, t.gamma, t.theta, t.vega, t.rho,
                 t.volume_24h, t.open_interest, t.underlying_price, t.timestamp)
                for t in tickers
            ]
            
            await conn.executemany("""
                INSERT INTO options_tickers 
                (instrument_id, exchange, mark_price, bid_price, ask_price, last_price,
                 iv, delta, gamma, theta, vega, rho, volume_24h, open_interest,
                 underlying_price, timestamp)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
                ON CONFLICT (instrument_id, timestamp) DO NOTHING
            """, values)
        
        return len(tickers)
    
    async def insert_historical_volatility(self, underlying: str, exchange: str, 
                                           period_days: int, realized_vol: float,
                                           timestamp: datetime) -> bool:
        """Insert historical volatility data."""
        async with self._pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO historical_volatility 
                (underlying, exchange, period_days, realized_vol, timestamp)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (underlying, exchange, period_days, timestamp) DO NOTHING
            """, underlying, exchange, period_days, realized_vol, timestamp)
        return True
    
    async def insert_volatility_surface(self, underlying: str, exchange: str,
                                        expiry: datetime, strike: float, option_type: str,
                                        iv: float, delta: float, underlying_price: float,
                                        timestamp: datetime) -> bool:
        """Insert volatility surface data point."""
        async with self._pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO volatility_surface
                (underlying, exchange, expiry, strike, option_type, iv, delta, underlying_price, timestamp)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                ON CONFLICT (underlying, exchange, expiry, strike, option_type, timestamp) DO NOTHING
            """, underlying, exchange, expiry, strike, option_type, iv, delta, underlying_price, timestamp)
        return True


class DeribitFetcher:
    """Fetch options data from Deribit public API."""
    
    BASE_URL = "https://www.deribit.com/api/v2/public"
    
    def __init__(self):
        self.timeout = httpx.Timeout(30.0)
    
    async def _request(self, endpoint: str, params: Dict = None) -> Optional[Any]:
        """Make async request."""
        url = f"{self.BASE_URL}/{endpoint}"
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                resp = await client.get(url, params=params)
                resp.raise_for_status()
                data = resp.json()
                return data.get("result")
        except Exception as e:
            logger.warning(f"Deribit request failed {endpoint}: {e}")
            return None
    
    async def get_index_price(self, currency: str = "BTC") -> Optional[float]:
        """Get current index price."""
        result = await self._request("get_index_price", {"index_name": f"{currency.lower()}_usd"})
        if result:
            return result.get("index_price")
        return None
    
    async def get_instruments(self, currency: str = "BTC", expired: bool = False) -> List[Dict]:
        """Get all option instruments."""
        result = await self._request("get_instruments", {
            "currency": currency, 
            "kind": "option",
            "expired": str(expired).lower()
        })
        return result if result else []
    
    async def get_book_summary(self, currency: str = "BTC") -> List[Dict]:
        """Get book summary with IV and greeks."""
        result = await self._request("get_book_summary_by_currency", {
            "currency": currency,
            "kind": "option"
        })
        return result if result else []
    
    async def get_historical_volatility(self, currency: str = "BTC") -> List[List]:
        """Get historical (realized) volatility."""
        result = await self._request("get_historical_volatility", {"currency": currency})
        return result if result else []
    
    def _parse_instrument_name(self, name: str) -> Tuple[str, datetime, float, str]:
        """Parse Deribit instrument name: BTC-28JUN24-70000-C"""
        parts = name.split("-")
        if len(parts) != 4:
            return None, None, None, None
        
        underlying = parts[0]
        expiry_str = parts[1]
        strike = float(parts[2])
        option_type = parts[3]  # C or P
        
        try:
            expiry = datetime.strptime(expiry_str, "%d%b%y")
            expiry = expiry.replace(hour=8, tzinfo=timezone.utc)
        except:
            return None, None, None, None
        
        return underlying, expiry, strike, option_type
    
    async def fetch_all(self, currencies: List[str] = ["BTC", "ETH"]) -> Tuple[List[OptionInstrument], List[OptionTicker], Dict]:
        """Fetch all options data from Deribit."""
        all_instruments = []
        all_tickers = []
        vol_data = {}
        
        for currency in currencies:
            logger.info(f"Deribit: Fetching {currency} options...")
            
            # Get index price
            index_price = await self.get_index_price(currency)
            if index_price is None:
                logger.warning(f"Could not get index price for {currency}")
                continue
            
            logger.info(f"Deribit {currency} index: ${index_price:,.2f}")
            
            # Get active instruments
            instruments = await self.get_instruments(currency, expired=False)
            await asyncio.sleep(0.1)  # Rate limit
            
            # Get expired instruments (historical)
            expired_instruments = await self.get_instruments(currency, expired=True)
            await asyncio.sleep(0.1)
            
            # Combine
            all_insts = instruments + expired_instruments
            logger.info(f"Deribit {currency}: {len(instruments)} active, {len(expired_instruments)} expired instruments")
            
            # Parse instruments
            for inst in all_insts:
                name = inst.get("instrument_name", "")
                underlying, expiry, strike, opt_type = self._parse_instrument_name(name)
                if expiry is None:
                    continue
                
                opt_inst = OptionInstrument(
                    instrument_id=name,
                    exchange="DERIBIT",
                    underlying=underlying,
                    strike=strike,
                    expiry=expiry,
                    option_type=opt_type,
                    settlement="cash",
                    contract_size=inst.get("contract_size"),
                    tick_size=inst.get("tick_size"),
                    is_active=inst.get("is_active", False),
                    raw_data=inst
                )
                all_instruments.append(opt_inst)
            
            # Get book summary (live data with greeks)
            summaries = await self.get_book_summary(currency)
            await asyncio.sleep(0.1)
            
            now = datetime.now(timezone.utc)
            for summary in summaries:
                name = summary.get("instrument_name", "")
                underlying, expiry, strike, opt_type = self._parse_instrument_name(name)
                if expiry is None:
                    continue
                
                # Parse greeks
                greeks = summary.get("greeks", {}) or {}
                
                # IV comes as percentage (e.g. 65.5), convert to decimal (0.655)
                iv_pct = summary.get("mark_iv")
                iv = iv_pct / 100 if iv_pct else None
                
                ticker = OptionTicker(
                    instrument_id=name,
                    exchange="DERIBIT",
                    mark_price=summary.get("mark_price"),
                    bid_price=summary.get("bid_price"),
                    ask_price=summary.get("ask_price"),
                    last_price=summary.get("last"),
                    iv=iv,
                    delta=greeks.get("delta"),
                    gamma=greeks.get("gamma"),
                    theta=greeks.get("theta"),
                    vega=greeks.get("vega"),
                    rho=greeks.get("rho"),
                    volume_24h=summary.get("volume"),
                    open_interest=summary.get("open_interest"),
                    underlying_price=index_price,
                    timestamp=now
                )
                all_tickers.append(ticker)
            
            # Get historical volatility
            hvol = await self.get_historical_volatility(currency)
            if hvol:
                vol_data[f"DERIBIT_{currency}"] = hvol
            
            await asyncio.sleep(0.2)
        
        logger.info(f"Deribit: {len(all_instruments)} instruments, {len(all_tickers)} tickers")
        return all_instruments, all_tickers, vol_data


class BinanceFetcher:
    """Fetch options data from Binance Options API."""
    
    BASE_URL = "https://eapi.binance.com/eapi/v1"
    
    def __init__(self):
        self.timeout = httpx.Timeout(30.0)
    
    async def _request(self, endpoint: str, params: Dict = None) -> Optional[Any]:
        """Make async request."""
        url = f"{self.BASE_URL}/{endpoint}"
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                resp = await client.get(url, params=params)
                resp.raise_for_status()
                return resp.json()
        except Exception as e:
            logger.warning(f"Binance request failed {endpoint}: {e}")
            return None
    
    async def get_exchange_info(self) -> Optional[Dict]:
        """Get exchange info with all symbols."""
        return await self._request("exchangeInfo")
    
    async def get_tickers(self) -> List[Dict]:
        """Get all option tickers."""
        result = await self._request("ticker")
        return result if result else []
    
    async def get_mark_prices(self) -> List[Dict]:
        """Get mark prices with greeks."""
        result = await self._request("mark")
        return result if result else []
    
    async def get_index(self, underlying: str = "BTCUSDT") -> Optional[float]:
        """Get underlying index price."""
        result = await self._request("index", {"underlying": underlying})
        if result:
            return float(result.get("indexPrice", 0))
        return None
    
    def _parse_symbol(self, symbol: str) -> Tuple[str, datetime, float, str]:
        """Parse Binance symbol: BTC-240628-70000-C"""
        parts = symbol.split("-")
        if len(parts) != 4:
            return None, None, None, None
        
        underlying = parts[0]
        expiry_str = parts[1]  # YYMMDD
        strike = float(parts[2])
        option_type = parts[3]  # C or P
        
        try:
            expiry = datetime.strptime(expiry_str, "%y%m%d")
            expiry = expiry.replace(hour=8, tzinfo=timezone.utc)
        except:
            return None, None, None, None
        
        return underlying, expiry, strike, option_type
    
    async def fetch_all(self, underlyings: List[str] = ["BTC", "ETH"]) -> Tuple[List[OptionInstrument], List[OptionTicker]]:
        """Fetch all options data from Binance."""
        all_instruments = []
        all_tickers = []
        
        logger.info("Binance: Fetching exchange info...")
        
        # Get exchange info (instruments)
        exchange_info = await self.get_exchange_info()
        if not exchange_info:
            logger.warning("Binance: Could not get exchange info")
            return [], []
        
        # Parse instruments from optionSymbols
        symbols = exchange_info.get("optionSymbols", [])
        logger.info(f"Binance: {len(symbols)} option symbols")
        
        for sym in symbols:
            symbol = sym.get("symbol", "")
            underlying, expiry, strike, opt_type = self._parse_symbol(symbol)
            if expiry is None:
                continue
            
            base = sym.get("baseAsset", "")
            if base not in underlyings:
                continue
            
            opt_inst = OptionInstrument(
                instrument_id=symbol,
                exchange="BINANCE",
                underlying=underlying,
                strike=strike,
                expiry=expiry,
                option_type=opt_type,
                settlement="cash",
                contract_size=float(sym.get("unit", 1)),
                tick_size=float(sym.get("minTradeAmount", 0.0001)),
                is_active=True,
                raw_data=sym
            )
            all_instruments.append(opt_inst)
        
        await asyncio.sleep(0.1)
        
        # Get tickers
        tickers = await self.get_tickers()
        await asyncio.sleep(0.1)
        
        # Get mark prices with greeks
        marks = await self.get_mark_prices()
        mark_lookup = {m.get("symbol"): m for m in marks} if marks else {}
        
        # Get index price
        btc_index = await self.get_index("BTCUSDT") or 0
        eth_index = await self.get_index("ETHUSDT") or 0
        index_prices = {"BTC": btc_index, "ETH": eth_index}
        
        now = datetime.now(timezone.utc)
        for ticker in tickers:
            symbol = ticker.get("symbol", "")
            underlying, expiry, strike, opt_type = self._parse_symbol(symbol)
            if expiry is None:
                continue
            
            if underlying not in underlyings:
                continue
            
            mark = mark_lookup.get(symbol, {})
            
            # IV comes as decimal from Binance (e.g. 0.65)
            iv = float(mark.get("markIV", 0)) if mark.get("markIV") else None
            
            opt_ticker = OptionTicker(
                instrument_id=symbol,
                exchange="BINANCE",
                mark_price=float(mark.get("markPrice", 0)) if mark.get("markPrice") else None,
                bid_price=float(ticker.get("bidPrice", 0)) if ticker.get("bidPrice") else None,
                ask_price=float(ticker.get("askPrice", 0)) if ticker.get("askPrice") else None,
                last_price=float(ticker.get("lastPrice", 0)) if ticker.get("lastPrice") else None,
                iv=iv,
                delta=float(mark.get("delta", 0)) if mark.get("delta") else None,
                gamma=float(mark.get("gamma", 0)) if mark.get("gamma") else None,
                theta=float(mark.get("theta", 0)) if mark.get("theta") else None,
                vega=float(mark.get("vega", 0)) if mark.get("vega") else None,
                volume_24h=float(ticker.get("volume", 0)) if ticker.get("volume") else None,
                open_interest=float(ticker.get("openInterest", 0)) if ticker.get("openInterest") else None,
                underlying_price=index_prices.get(underlying, 0),
                timestamp=now
            )
            all_tickers.append(opt_ticker)
        
        logger.info(f"Binance: {len(all_instruments)} instruments, {len(all_tickers)} tickers")
        return all_instruments, all_tickers


class OKXFetcher:
    """Fetch options data from OKX public API."""
    
    BASE_URL = "https://www.okx.com/api/v5"
    
    def __init__(self):
        self.timeout = httpx.Timeout(30.0)
    
    async def _request(self, endpoint: str, params: Dict = None) -> Optional[List]:
        """Make async request."""
        url = f"{self.BASE_URL}/{endpoint}"
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                resp = await client.get(url, params=params)
                resp.raise_for_status()
                data = resp.json()
                if data.get("code") == "0":
                    return data.get("data", [])
                logger.warning(f"OKX error: {data.get('msg')}")
                return None
        except Exception as e:
            logger.warning(f"OKX request failed {endpoint}: {e}")
            return None
    
    async def get_instruments(self, uly: str = "BTC-USD") -> List[Dict]:
        """Get all option instruments."""
        result = await self._request("public/instruments", {
            "instType": "OPTION",
            "uly": uly
        })
        return result if result else []
    
    async def get_opt_summary(self, uly: str = "BTC-USD") -> List[Dict]:
        """Get option summary with greeks and IV."""
        result = await self._request("public/opt-summary", {"uly": uly})
        return result if result else []
    
    async def get_tickers(self, uly: str = "BTC-USD") -> List[Dict]:
        """Get option tickers."""
        result = await self._request("market/tickers", {
            "instType": "OPTION",
            "uly": uly
        })
        return result if result else []
    
    async def get_index_price(self, instId: str = "BTC-USD") -> Optional[float]:
        """Get index price."""
        result = await self._request("market/index-tickers", {"instId": instId})
        if result and len(result) > 0:
            return float(result[0].get("idxPx", 0))
        return None
    
    def _parse_inst_id(self, inst_id: str) -> Tuple[str, datetime, float, str]:
        """Parse OKX instId: BTC-USD-240628-70000-C"""
        parts = inst_id.split("-")
        if len(parts) != 5:
            return None, None, None, None
        
        underlying = parts[0]
        # parts[1] is USD
        expiry_str = parts[2]  # YYMMDD
        strike = float(parts[3])
        option_type = parts[4]  # C or P
        
        try:
            expiry = datetime.strptime(expiry_str, "%y%m%d")
            expiry = expiry.replace(hour=8, tzinfo=timezone.utc)
        except:
            return None, None, None, None
        
        return underlying, expiry, strike, option_type
    
    async def fetch_all(self, underlyings: List[str] = ["BTC", "ETH"]) -> Tuple[List[OptionInstrument], List[OptionTicker]]:
        """Fetch all options data from OKX."""
        all_instruments = []
        all_tickers = []
        
        for underlying in underlyings:
            uly = f"{underlying}-USD"
            logger.info(f"OKX: Fetching {uly} options...")
            
            # Get index price
            index_price = await self.get_index_price(uly) or 0
            logger.info(f"OKX {underlying} index: ${index_price:,.2f}")
            
            # Get instruments
            instruments = await self.get_instruments(uly)
            await asyncio.sleep(0.1)
            
            logger.info(f"OKX {underlying}: {len(instruments)} instruments")
            
            for inst in instruments:
                inst_id = inst.get("instId", "")
                base, expiry, strike, opt_type = self._parse_inst_id(inst_id)
                if expiry is None:
                    continue
                
                opt_inst = OptionInstrument(
                    instrument_id=inst_id,
                    exchange="OKX",
                    underlying=underlying,
                    strike=strike,
                    expiry=expiry,
                    option_type=opt_type,
                    settlement="cash",
                    contract_size=float(inst.get("ctVal", 1)),
                    tick_size=float(inst.get("tickSz", 0.0001)),
                    is_active=inst.get("state") == "live",
                    raw_data=inst
                )
                all_instruments.append(opt_inst)
            
            # Get opt summary (greeks + IV)
            summaries = await self.get_opt_summary(uly)
            await asyncio.sleep(0.1)
            
            summary_lookup = {s.get("instId"): s for s in summaries}
            
            # Get tickers
            tickers = await self.get_tickers(uly)
            await asyncio.sleep(0.1)
            
            now = datetime.now(timezone.utc)
            for ticker in tickers:
                inst_id = ticker.get("instId", "")
                base, expiry, strike, opt_type = self._parse_inst_id(inst_id)
                if expiry is None:
                    continue
                
                summary = summary_lookup.get(inst_id, {})
                
                # markVol comes as decimal string (e.g. "0.65")
                iv_str = summary.get("markVol") or summary.get("impVol")
                iv = float(iv_str) if iv_str else None
                
                opt_ticker = OptionTicker(
                    instrument_id=inst_id,
                    exchange="OKX",
                    mark_price=float(ticker.get("last", 0)) if ticker.get("last") else None,
                    bid_price=float(ticker.get("bidPx", 0)) if ticker.get("bidPx") else None,
                    ask_price=float(ticker.get("askPx", 0)) if ticker.get("askPx") else None,
                    last_price=float(ticker.get("last", 0)) if ticker.get("last") else None,
                    iv=iv,
                    delta=float(summary.get("delta", 0)) if summary.get("delta") else None,
                    gamma=float(summary.get("gamma", 0)) if summary.get("gamma") else None,
                    theta=float(summary.get("theta", 0)) if summary.get("theta") else None,
                    vega=float(summary.get("vega", 0)) if summary.get("vega") else None,
                    volume_24h=float(ticker.get("vol24h", 0)) if ticker.get("vol24h") else None,
                    open_interest=float(summary.get("oi", 0)) if summary.get("oi") else None,
                    underlying_price=index_price,
                    timestamp=now
                )
                all_tickers.append(opt_ticker)
            
            await asyncio.sleep(0.2)
        
        logger.info(f"OKX: {len(all_instruments)} instruments, {len(all_tickers)} tickers")
        return all_instruments, all_tickers


async def main():
    """Main entry point."""
    logger.info("=" * 60)
    logger.info("OPTIONS DATA PIPELINE")
    logger.info("=" * 60)
    
    # Initialize database
    db = DatabaseClient(DATABASE_URL)
    await db.init_pool()
    
    # Initialize fetchers
    deribit = DeribitFetcher()
    binance = BinanceFetcher()
    okx = OKXFetcher()
    
    currencies = ["BTC", "ETH"]
    
    # Stats
    stats = {
        "instruments": {"DERIBIT": 0, "BINANCE": 0, "OKX": 0},
        "tickers": {"DERIBIT": 0, "BINANCE": 0, "OKX": 0}
    }
    
    try:
        # Fetch from Deribit
        logger.info("\n" + "=" * 40)
        logger.info("DERIBIT")
        logger.info("=" * 40)
        d_instruments, d_tickers, d_vol = await deribit.fetch_all(currencies)
        
        # Store Deribit data
        if d_instruments:
            count = await db.upsert_instruments_batch(d_instruments)
            stats["instruments"]["DERIBIT"] = count
            logger.info(f"Stored {count} Deribit instruments")
        
        if d_tickers:
            count = await db.insert_tickers_batch(d_tickers)
            stats["tickers"]["DERIBIT"] = count
            logger.info(f"Stored {count} Deribit tickers")
        
        # Store historical volatility
        for key, hvol in d_vol.items():
            for ts, vol in hvol:
                timestamp = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
                currency = key.split("_")[1]
                await db.insert_historical_volatility(currency, "DERIBIT", 30, vol / 100, timestamp)
        
        # Fetch from Binance
        logger.info("\n" + "=" * 40)
        logger.info("BINANCE")
        logger.info("=" * 40)
        b_instruments, b_tickers = await binance.fetch_all(currencies)
        
        if b_instruments:
            count = await db.upsert_instruments_batch(b_instruments)
            stats["instruments"]["BINANCE"] = count
            logger.info(f"Stored {count} Binance instruments")
        
        if b_tickers:
            count = await db.insert_tickers_batch(b_tickers)
            stats["tickers"]["BINANCE"] = count
            logger.info(f"Stored {count} Binance tickers")
        
        # Fetch from OKX
        logger.info("\n" + "=" * 40)
        logger.info("OKX")
        logger.info("=" * 40)
        o_instruments, o_tickers = await okx.fetch_all(currencies)
        
        if o_instruments:
            count = await db.upsert_instruments_batch(o_instruments)
            stats["instruments"]["OKX"] = count
            logger.info(f"Stored {count} OKX instruments")
        
        if o_tickers:
            count = await db.insert_tickers_batch(o_tickers)
            stats["tickers"]["OKX"] = count
            logger.info(f"Stored {count} OKX tickers")
        
        # Store volatility surface data
        logger.info("\n" + "=" * 40)
        logger.info("STORING VOLATILITY SURFACE")
        logger.info("=" * 40)
        
        all_tickers = d_tickers + b_tickers + o_tickers
        now = datetime.now(timezone.utc)
        surface_count = 0
        
        for ticker in all_tickers:
            if ticker.iv is None or ticker.iv <= 0:
                continue
            
            # Find corresponding instrument for expiry
            _, expiry, strike, opt_type = None, None, None, None
            
            if ticker.exchange == "DERIBIT":
                _, expiry, strike, opt_type = deribit._parse_instrument_name(ticker.instrument_id)
            elif ticker.exchange == "BINANCE":
                _, expiry, strike, opt_type = binance._parse_symbol(ticker.instrument_id)
            elif ticker.exchange == "OKX":
                _, expiry, strike, opt_type = okx._parse_inst_id(ticker.instrument_id)
            
            if expiry is None:
                continue
            
            # Extract underlying from instrument_id
            underlying = ticker.instrument_id.split("-")[0]
            
            await db.insert_volatility_surface(
                underlying=underlying,
                exchange=ticker.exchange,
                expiry=expiry,
                strike=strike,
                option_type=opt_type,
                iv=ticker.iv,
                delta=ticker.delta or 0,
                underlying_price=ticker.underlying_price or 0,
                timestamp=now
            )
            surface_count += 1
        
        logger.info(f"Stored {surface_count} volatility surface points")
        
        # Summary
        logger.info("\n" + "=" * 60)
        logger.info("SUMMARY")
        logger.info("=" * 60)
        
        total_instruments = sum(stats["instruments"].values())
        total_tickers = sum(stats["tickers"].values())
        
        logger.info(f"Total instruments: {total_instruments}")
        for exchange, count in stats["instruments"].items():
            logger.info(f"  {exchange}: {count}")
        
        logger.info(f"\nTotal tickers: {total_tickers}")
        for exchange, count in stats["tickers"].items():
            logger.info(f"  {exchange}: {count}")
        
        logger.info(f"\nVolatility surface points: {surface_count}")
        
        return stats
        
    finally:
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())
