"""
Options Data Fetcher — Real-time data from Deribit & Binance public APIs

No API keys required - uses public endpoints only.
"""

import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field

import httpx

from . import config

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


@dataclass
class OptionData:
    """Standardized option data from any exchange."""
    symbol: str                     # e.g., "BTC-28JUN24-70000-C"
    underlying: str                 # e.g., "BTC"
    strike: float                   # Strike price
    expiry: datetime                # Expiration datetime
    option_type: str                # "call" or "put"
    
    # Prices
    bid: Optional[float] = None
    ask: Optional[float] = None
    mark_price: Optional[float] = None
    last_price: Optional[float] = None
    
    # Volatility
    iv: Optional[float] = None      # Implied volatility (annualized)
    
    # Greeks
    delta: Optional[float] = None
    gamma: Optional[float] = None
    theta: Optional[float] = None
    vega: Optional[float] = None
    
    # Volume & OI
    volume_24h: Optional[float] = None
    open_interest: Optional[float] = None
    
    # Metadata
    exchange: str = ""
    dte: float = 0.0               # Days to expiry
    moneyness: float = 0.0         # S/K ratio
    
    # Raw data
    raw: Dict = field(default_factory=dict)


@dataclass
class UnderlyingData:
    """Underlying asset data."""
    symbol: str                     # e.g., "BTC"
    index_price: float              # Index/spot price
    mark_price: Optional[float] = None
    
    # Volatility data
    realized_vol_7d: Optional[float] = None
    realized_vol_30d: Optional[float] = None
    realized_vol_60d: Optional[float] = None
    
    # Price history (for RV calculation)
    price_history: List[Tuple[datetime, float]] = field(default_factory=list)
    
    exchange: str = ""
    timestamp: Optional[datetime] = None


class DeribitFetcher:
    """Fetch option data from Deribit public API."""
    
    def __init__(self):
        self.base_url = config.DERIBIT_BASE
        self.timeout = httpx.Timeout(30.0)
        self.rate_limit_sleep = config.API_SLEEP_MS / 1000
    
    async def _request(self, endpoint: str, params: Dict = None) -> Optional[Dict]:
        """Make async request to Deribit API."""
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.get(endpoint, params=params)
                response.raise_for_status()
                data = response.json()
                if data.get("result") is not None:
                    return data["result"]
                return data
        except httpx.HTTPError as e:
            logger.error(f"Deribit API error: {e}")
            return None
        except Exception as e:
            logger.error(f"Deribit request failed: {e}")
            return None
    
    async def get_index_price(self, currency: str = "BTC") -> Optional[float]:
        """Get current index price for BTC or ETH."""
        result = await self._request(
            config.DERIBIT_ENDPOINTS["index_price"],
            {"index_name": f"{currency.lower()}_usd"}
        )
        if result:
            return result.get("index_price")
        return None
    
    async def get_instruments(self, currency: str = "BTC", kind: str = "option",
                              expired: bool = False) -> List[Dict]:
        """Get all available option instruments."""
        result = await self._request(
            config.DERIBIT_ENDPOINTS["instruments"],
            {"currency": currency, "kind": kind, "expired": str(expired).lower()}
        )
        return result if result else []
    
    async def get_book_summary(self, currency: str = "BTC", kind: str = "option") -> List[Dict]:
        """Get book summary with IV and greeks for all options."""
        result = await self._request(
            config.DERIBIT_ENDPOINTS["book_summary"],
            {"currency": currency, "kind": kind}
        )
        return result if result else []
    
    async def get_ticker(self, instrument_name: str) -> Optional[Dict]:
        """Get ticker for specific instrument."""
        result = await self._request(
            config.DERIBIT_ENDPOINTS["ticker"],
            {"instrument_name": instrument_name}
        )
        return result
    
    async def get_historical_volatility(self, currency: str = "BTC") -> Optional[Dict]:
        """Get historical (realized) volatility."""
        result = await self._request(
            config.DERIBIT_ENDPOINTS["historical_vol"],
            {"currency": currency}
        )
        return result
    
    async def get_chart_data(self, instrument_name: str, resolution: str = "1D",
                             start_timestamp: int = None, end_timestamp: int = None) -> Optional[Dict]:
        """Get OHLCV data for underlying or option."""
        if start_timestamp is None:
            start_timestamp = int((datetime.now(timezone.utc) - timedelta(days=90)).timestamp() * 1000)
        if end_timestamp is None:
            end_timestamp = int(datetime.now(timezone.utc).timestamp() * 1000)
        
        result = await self._request(
            config.DERIBIT_ENDPOINTS["chart_data"],
            {
                "instrument_name": instrument_name,
                "resolution": resolution,
                "start_timestamp": start_timestamp,
                "end_timestamp": end_timestamp,
            }
        )
        return result
    
    def _parse_instrument_name(self, name: str) -> Tuple[str, datetime, float, str]:
        """
        Parse Deribit instrument name.
        e.g., "BTC-28JUN24-70000-C" -> (BTC, datetime, 70000, call)
        """
        parts = name.split("-")
        if len(parts) != 4:
            return None, None, None, None
        
        underlying = parts[0]
        expiry_str = parts[1]
        strike = float(parts[2])
        option_type = "call" if parts[3] == "C" else "put"
        
        # Parse expiry: 28JUN24 format
        try:
            expiry = datetime.strptime(expiry_str, "%d%b%y")
            expiry = expiry.replace(hour=8, tzinfo=timezone.utc)  # Deribit expires at 08:00 UTC
        except:
            expiry = None
        
        return underlying, expiry, strike, option_type
    
    async def fetch_all_options(self, currency: str = "BTC", 
                                 spot_price: float = None) -> List[OptionData]:
        """
        Fetch all available options with full data.
        
        Returns list of OptionData objects.
        """
        # Get spot price if not provided
        if spot_price is None:
            spot_price = await self.get_index_price(currency)
            if spot_price is None:
                logger.error(f"Could not get index price for {currency}")
                return []
        
        # Get book summary (has IV and greeks)
        summaries = await self.get_book_summary(currency, "option")
        if not summaries:
            logger.warning(f"No option summaries found for {currency}")
            return []
        
        options = []
        now = datetime.now(timezone.utc)
        
        for summary in summaries:
            try:
                name = summary.get("instrument_name", "")
                underlying, expiry, strike, option_type = self._parse_instrument_name(name)
                
                if expiry is None or expiry <= now:
                    continue  # Skip expired
                
                dte = (expiry - now).total_seconds() / 86400
                if dte < config.MIN_DTE:
                    continue
                
                opt = OptionData(
                    symbol=name,
                    underlying=underlying,
                    strike=strike,
                    expiry=expiry,
                    option_type=option_type,
                    bid=summary.get("bid_price"),
                    ask=summary.get("ask_price"),
                    mark_price=summary.get("mark_price"),
                    last_price=summary.get("last"),
                    iv=summary.get("mark_iv"),
                    delta=summary.get("greeks", {}).get("delta") if isinstance(summary.get("greeks"), dict) else None,
                    gamma=summary.get("greeks", {}).get("gamma") if isinstance(summary.get("greeks"), dict) else None,
                    theta=summary.get("greeks", {}).get("theta") if isinstance(summary.get("greeks"), dict) else None,
                    vega=summary.get("greeks", {}).get("vega") if isinstance(summary.get("greeks"), dict) else None,
                    volume_24h=summary.get("volume"),
                    open_interest=summary.get("open_interest"),
                    exchange="deribit",
                    dte=dte,
                    moneyness=spot_price / strike if strike > 0 else 0,
                    raw=summary,
                )
                
                # Get greeks from raw if not in summary
                if opt.delta is None and "greeks" in summary:
                    greeks = summary["greeks"]
                    if isinstance(greeks, dict):
                        opt.delta = greeks.get("delta")
                        opt.gamma = greeks.get("gamma")
                        opt.theta = greeks.get("theta")
                        opt.vega = greeks.get("vega")
                
                options.append(opt)
                
            except Exception as e:
                logger.debug(f"Failed to parse option {summary}: {e}")
                continue
        
        logger.info(f"Deribit: fetched {len(options)} {currency} options")
        return options
    
    async def fetch_underlying_data(self, currency: str = "BTC") -> Optional[UnderlyingData]:
        """Fetch underlying price and volatility data."""
        # Get index price
        index_price = await self.get_index_price(currency)
        if index_price is None:
            return None
        
        underlying = UnderlyingData(
            symbol=currency,
            index_price=index_price,
            exchange="deribit",
            timestamp=datetime.now(timezone.utc),
        )
        
        # Get historical volatility
        hvol = await self.get_historical_volatility(currency)
        if hvol:
            # Deribit returns list of [timestamp, volatility] pairs
            if isinstance(hvol, list) and len(hvol) > 0:
                # Most recent volatility
                underlying.realized_vol_30d = hvol[-1][1] / 100 if len(hvol[-1]) > 1 else None
        
        # Get price history from chart data
        chart = await self.get_chart_data(f"{currency}_USDC", "1D")
        if chart and "close" in chart:
            closes = chart["close"]
            timestamps = chart.get("ticks", [])
            underlying.price_history = [
                (datetime.fromtimestamp(ts/1000, tz=timezone.utc), price)
                for ts, price in zip(timestamps, closes)
                if price is not None
            ]
        
        return underlying


class BinanceFetcher:
    """Fetch option data from Binance Options public API."""
    
    def __init__(self):
        self.base_url = config.BINANCE_BASE
        self.timeout = httpx.Timeout(30.0)
        self.rate_limit_sleep = config.API_SLEEP_MS / 1000
    
    async def _request(self, endpoint: str, params: Dict = None) -> Optional[Any]:
        """Make async request to Binance API."""
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.get(endpoint, params=params)
                response.raise_for_status()
                return response.json()
        except httpx.HTTPError as e:
            logger.warning(f"Binance API error: {e}")
            return None
        except Exception as e:
            logger.warning(f"Binance request failed: {e}")
            return None
    
    async def get_exchange_info(self) -> Optional[Dict]:
        """Get exchange info with all option symbols."""
        return await self._request(config.BINANCE_ENDPOINTS["exchange_info"])
    
    async def get_tickers(self) -> List[Dict]:
        """Get all option tickers."""
        result = await self._request(config.BINANCE_ENDPOINTS["ticker"])
        return result if result else []
    
    async def get_mark_prices(self) -> List[Dict]:
        """Get mark prices with greeks."""
        result = await self._request(config.BINANCE_ENDPOINTS["mark"])
        return result if result else []
    
    def _parse_symbol(self, symbol: str) -> Tuple[str, datetime, float, str]:
        """
        Parse Binance option symbol.
        e.g., "BTC-240628-70000-C" -> (BTC, datetime, 70000, call)
        """
        parts = symbol.split("-")
        if len(parts) != 4:
            return None, None, None, None
        
        underlying = parts[0]
        expiry_str = parts[1]  # YYMMDD format
        strike = float(parts[2])
        option_type = "call" if parts[3] == "C" else "put"
        
        try:
            expiry = datetime.strptime(expiry_str, "%y%m%d")
            expiry = expiry.replace(hour=8, tzinfo=timezone.utc)
        except:
            expiry = None
        
        return underlying, expiry, strike, option_type
    
    async def fetch_all_options(self, underlying: str = "BTC",
                                 spot_price: float = None) -> List[OptionData]:
        """
        Fetch all available options with full data.
        """
        # Get tickers and mark prices
        tickers = await self.get_tickers()
        mark_prices = await self.get_mark_prices()
        
        # Build mark price lookup
        mark_lookup = {m.get("symbol"): m for m in mark_prices}
        
        options = []
        now = datetime.now(timezone.utc)
        
        for ticker in tickers:
            try:
                symbol = ticker.get("symbol", "")
                if not symbol.startswith(underlying):
                    continue
                
                underlying_name, expiry, strike, option_type = self._parse_symbol(symbol)
                
                if expiry is None or expiry <= now:
                    continue
                
                dte = (expiry - now).total_seconds() / 86400
                if dte < config.MIN_DTE:
                    continue
                
                mark = mark_lookup.get(symbol, {})
                
                opt = OptionData(
                    symbol=symbol,
                    underlying=underlying_name,
                    strike=strike,
                    expiry=expiry,
                    option_type=option_type,
                    bid=float(ticker.get("bidPrice", 0)) if ticker.get("bidPrice") else None,
                    ask=float(ticker.get("askPrice", 0)) if ticker.get("askPrice") else None,
                    mark_price=float(mark.get("markPrice", 0)) if mark.get("markPrice") else None,
                    last_price=float(ticker.get("lastPrice", 0)) if ticker.get("lastPrice") else None,
                    iv=float(mark.get("markIV", 0)) if mark.get("markIV") else None,
                    delta=float(mark.get("delta", 0)) if mark.get("delta") else None,
                    gamma=float(mark.get("gamma", 0)) if mark.get("gamma") else None,
                    theta=float(mark.get("theta", 0)) if mark.get("theta") else None,
                    vega=float(mark.get("vega", 0)) if mark.get("vega") else None,
                    volume_24h=float(ticker.get("volume", 0)) if ticker.get("volume") else None,
                    open_interest=float(ticker.get("openInterest", 0)) if ticker.get("openInterest") else None,
                    exchange="binance",
                    dte=dte,
                    moneyness=spot_price / strike if spot_price and strike > 0 else 0,
                    raw=ticker,
                )
                
                options.append(opt)
                
            except Exception as e:
                logger.debug(f"Failed to parse Binance option {ticker}: {e}")
                continue
        
        logger.info(f"Binance: fetched {len(options)} {underlying} options")
        return options


class OptionDataFetcher:
    """
    Unified data fetcher combining Deribit and Binance.
    """
    
    def __init__(self):
        self.deribit = DeribitFetcher()
        self.binance = BinanceFetcher()
    
    async def fetch_all_data(self, currency: str = "BTC") -> Dict[str, Any]:
        """
        Fetch all option data from both exchanges.
        
        Returns dict with:
            - underlying: UnderlyingData
            - options: List[OptionData]
            - iv_surface: IV data by strike/expiry
            - realized_vol: Historical volatility data
        """
        logger.info(f"Fetching {currency} option data from exchanges...")
        
        # Fetch underlying first
        underlying = await self.deribit.fetch_underlying_data(currency)
        if underlying is None:
            logger.error("Failed to fetch underlying data")
            return {"underlying": None, "options": [], "iv_surface": {}, "realized_vol": {}}
        
        spot_price = underlying.index_price
        logger.info(f"{currency} index price: ${spot_price:,.2f}")
        
        # Fetch options from both exchanges
        deribit_options, binance_options = await asyncio.gather(
            self.deribit.fetch_all_options(currency, spot_price),
            self.binance.fetch_all_options(currency, spot_price),
            return_exceptions=True
        )
        
        # Handle exceptions
        if isinstance(deribit_options, Exception):
            logger.error(f"Deribit fetch failed: {deribit_options}")
            deribit_options = []
        if isinstance(binance_options, Exception):
            logger.warning(f"Binance fetch failed: {binance_options}")
            binance_options = []
        
        all_options = list(deribit_options) + list(binance_options)
        
        # Build IV surface (strike -> expiry -> IV)
        iv_surface = self._build_iv_surface(all_options, spot_price)
        
        # Calculate realized volatility if we have price history
        realized_vol = {}
        if underlying.price_history:
            prices = [p[1] for p in underlying.price_history]
            from . import pricing
            realized_vol = {
                "7d": pricing.calculate_realized_volatility(prices, 7),
                "30d": pricing.calculate_realized_volatility(prices, 30),
                "60d": pricing.calculate_realized_volatility(prices, 60),
            }
            underlying.realized_vol_7d = realized_vol.get("7d")
            underlying.realized_vol_30d = realized_vol.get("30d")
            underlying.realized_vol_60d = realized_vol.get("60d")
        
        return {
            "underlying": underlying,
            "options": all_options,
            "iv_surface": iv_surface,
            "realized_vol": realized_vol,
        }
    
    def _build_iv_surface(self, options: List[OptionData], spot_price: float) -> Dict:
        """
        Build IV surface for visualization.
        
        Returns: {
            "calls": {expiry_str: [(moneyness, iv), ...]},
            "puts": {expiry_str: [(moneyness, iv), ...]},
            "atm_term_structure": [(dte, atm_iv), ...],
        }
        """
        surface = {"calls": {}, "puts": {}, "atm_term_structure": []}
        
        # Group by expiry
        by_expiry = {}
        for opt in options:
            if opt.iv is None or opt.iv <= 0:
                continue
            
            expiry_str = opt.expiry.strftime("%Y-%m-%d")
            if expiry_str not in by_expiry:
                by_expiry[expiry_str] = {"calls": [], "puts": [], "dte": opt.dte}
            
            moneyness = spot_price / opt.strike if opt.strike > 0 else 1.0
            
            if opt.option_type == "call":
                by_expiry[expiry_str]["calls"].append((moneyness, opt.iv))
            else:
                by_expiry[expiry_str]["puts"].append((moneyness, opt.iv))
        
        # Populate surface
        for expiry_str, data in sorted(by_expiry.items()):
            surface["calls"][expiry_str] = sorted(data["calls"], key=lambda x: x[0])
            surface["puts"][expiry_str] = sorted(data["puts"], key=lambda x: x[0])
            
            # Find ATM IV (moneyness closest to 1.0)
            all_ivs = data["calls"] + data["puts"]
            if all_ivs:
                atm = min(all_ivs, key=lambda x: abs(x[0] - 1.0))
                surface["atm_term_structure"].append((data["dte"], atm[1]))
        
        surface["atm_term_structure"].sort(key=lambda x: x[0])
        
        return surface
    
    def select_options_for_strategy(self, options: List[OptionData], spot_price: float,
                                     strategy: str, target_dte: int = None) -> Dict[str, OptionData]:
        """
        Select specific options for a strategy.
        
        Strategies:
            - "covered_call": OTM call with delta ~0.30
            - "cash_secured_put": OTM put with delta ~-0.30
            - "short_strangle": OTM call + put with delta ~0.15
            - "iron_condor": 4 legs (inner + outer wings)
        
        Returns dict of selected options by role.
        """
        if target_dte is None:
            target_dte = config.TARGET_DTE
        
        # Filter by DTE
        candidates = [o for o in options if config.MIN_DTE <= o.dte <= config.MAX_DTE]
        if not candidates:
            candidates = options
        
        # Find closest expiry to target DTE
        if candidates:
            target_expiry = min(candidates, key=lambda o: abs(o.dte - target_dte)).expiry
            candidates = [o for o in candidates if o.expiry == target_expiry]
        
        # Separate calls and puts
        calls = sorted([o for o in candidates if o.option_type == "call"], 
                       key=lambda o: o.strike)
        puts = sorted([o for o in candidates if o.option_type == "put"], 
                      key=lambda o: o.strike, reverse=True)
        
        selected = {}
        
        if strategy == "covered_call":
            # Find call with delta closest to target
            if calls:
                selected["short_call"] = min(
                    [c for c in calls if c.delta is not None],
                    key=lambda c: abs(abs(c.delta) - config.OTM_DELTA_CALL),
                    default=calls[len(calls)//2]
                )
        
        elif strategy == "cash_secured_put":
            # Find put with delta closest to target (negative)
            if puts:
                selected["short_put"] = min(
                    [p for p in puts if p.delta is not None],
                    key=lambda p: abs(abs(p.delta) - abs(config.OTM_DELTA_PUT)),
                    default=puts[0]
                )
        
        elif strategy == "short_strangle":
            # OTM call + OTM put
            if calls:
                selected["short_call"] = min(
                    [c for c in calls if c.delta is not None and c.strike > spot_price],
                    key=lambda c: abs(abs(c.delta) - config.OTM_DELTA_STRANGLE),
                    default=None
                )
            if puts:
                selected["short_put"] = min(
                    [p for p in puts if p.delta is not None and p.strike < spot_price],
                    key=lambda p: abs(abs(p.delta) - config.OTM_DELTA_STRANGLE),
                    default=None
                )
        
        elif strategy == "iron_condor":
            # 4 legs: sell inner call/put, buy outer call/put
            if calls:
                inner_calls = [c for c in calls if c.delta is not None and c.strike > spot_price]
                if inner_calls:
                    selected["short_call"] = min(
                        inner_calls,
                        key=lambda c: abs(abs(c.delta) - config.OTM_DELTA_CONDOR_INNER),
                        default=None
                    )
                    if selected.get("short_call"):
                        # Find outer call - any call with higher strike
                        outer_calls = [c for c in calls if c.strike > selected["short_call"].strike]
                        if outer_calls:
                            # Prefer options with delta, but fall back to any
                            outer_with_delta = [c for c in outer_calls if c.delta is not None]
                            if outer_with_delta:
                                selected["long_call"] = min(
                                    outer_with_delta,
                                    key=lambda c: abs(abs(c.delta) - config.OTM_DELTA_CONDOR_OUTER)
                                )
                            else:
                                # Just pick the nearest OTM call
                                selected["long_call"] = min(outer_calls, key=lambda c: c.strike)
            
            if puts:
                inner_puts = [p for p in puts if p.delta is not None and p.strike < spot_price]
                if inner_puts:
                    selected["short_put"] = min(
                        inner_puts,
                        key=lambda p: abs(abs(p.delta) - config.OTM_DELTA_CONDOR_INNER),
                        default=None
                    )
                    if selected.get("short_put"):
                        # Find outer put - any put with lower strike
                        outer_puts = [p for p in puts if p.strike < selected["short_put"].strike]
                        if outer_puts:
                            # Prefer options with delta, but fall back to any
                            outer_with_delta = [p for p in outer_puts if p.delta is not None]
                            if outer_with_delta:
                                selected["long_put"] = min(
                                    outer_with_delta,
                                    key=lambda p: abs(abs(p.delta) - config.OTM_DELTA_CONDOR_OUTER)
                                )
                            else:
                                # Just pick the nearest OTM put
                                selected["long_put"] = max(outer_puts, key=lambda p: p.strike)
        
        return selected


async def test_fetcher():
    """Test data fetching."""
    fetcher = OptionDataFetcher()
    data = await fetcher.fetch_all_data("BTC")
    
    print(f"\nUnderlying: {data['underlying']}")
    print(f"Total options: {len(data['options'])}")
    print(f"Realized vol: {data['realized_vol']}")
    
    if data['options']:
        # Show some sample options
        calls = [o for o in data['options'] if o.option_type == 'call'][:5]
        puts = [o for o in data['options'] if o.option_type == 'put'][:5]
        
        print("\nSample calls:")
        for c in calls:
            print(f"  {c.symbol}: strike={c.strike}, IV={c.iv:.1f}%, delta={c.delta}")
        
        print("\nSample puts:")
        for p in puts:
            print(f"  {p.symbol}: strike={p.strike}, IV={p.iv:.1f}%, delta={p.delta}")


if __name__ == "__main__":
    asyncio.run(test_fetcher())
