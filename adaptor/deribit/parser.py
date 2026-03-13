"""
Deribit Parser — Data Parsing Layer

Converts raw Deribit API responses into standardized formats
matching our database schema (options_instruments, options_tickers, volatility_surface).
"""

from typing import Any, Dict, List, Optional, Type
from datetime import datetime, timezone


class DeribitParser:
    """Parser for Deribit API responses."""

    @staticmethod
    def as_type(value: Any, to_type: Type[Any], default: Any = None) -> Any:
        """Safely convert value to target type."""
        if value is None or value == '':
            return default if default is not None else (to_type() if to_type != float else 0.0)
        try:
            return to_type(value)
        except (ValueError, TypeError):
            return default if default is not None else (to_type() if to_type != float else 0.0)

    @staticmethod
    def ms_to_datetime(ms_timestamp: Any) -> Optional[datetime]:
        """Convert millisecond timestamp to datetime."""
        if ms_timestamp is None or ms_timestamp == '' or ms_timestamp == 0:
            return None
        try:
            ts = int(ms_timestamp) / 1000.0
            return datetime.fromtimestamp(ts, tz=timezone.utc)
        except (ValueError, TypeError, OSError):
            return None

    @staticmethod
    def parse_instrument_name(instrument_name: str) -> Dict[str, Any]:
        """
        Parse Deribit instrument name into components.

        Options:  "BTC-28MAR25-100000-C"  → underlying=BTC, expiry_str=28MAR25, strike=100000, type=C
        Futures:  "BTC-28MAR25"           → underlying=BTC, expiry_str=28MAR25
        Perp:     "BTC-PERPETUAL"         → underlying=BTC
        """
        parts = instrument_name.split("-")
        result = {"underlying": parts[0] if parts else ""}

        if len(parts) == 4:
            # Option: UNDERLYING-EXPIRY-STRIKE-TYPE
            result["expiry_str"] = parts[1]
            result["strike"] = float(parts[2])
            result["option_type"] = parts[3]  # "C" or "P"
            result["kind"] = "option"
        elif len(parts) == 2 and parts[1] == "PERPETUAL":
            result["kind"] = "perpetual"
        elif len(parts) == 2:
            result["expiry_str"] = parts[1]
            result["kind"] = "future"
        else:
            result["kind"] = "unknown"

        return result

    # ==================== Instrument Parsing ====================

    def parse_instrument(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse a single instrument from /public/get_instruments.

        Deribit fields:
        - instrument_name: "BTC-28MAR25-100000-C"
        - kind: "option"
        - base_currency: "BTC"
        - quote_currency: "USD"
        - strike: 100000.0
        - option_type: "call" / "put"
        - settlement_period: "week" / "month" / "day"
        - contract_size: 1.0 (in base currency)
        - min_trade_amount: 0.1
        - tick_size: 0.0005
        - is_active: true
        - creation_timestamp: ms
        - expiration_timestamp: ms
        """
        symbol = self.as_type(raw.get("instrument_name"), str, "")
        underlying = self.as_type(raw.get("base_currency"), str, "")
        option_type_raw = raw.get("option_type", "")

        # Deribit uses "call"/"put"; we store "C"/"P"
        if option_type_raw == "call":
            option_type = "C"
        elif option_type_raw == "put":
            option_type = "P"
        else:
            option_type = option_type_raw.upper()[:1] if option_type_raw else ""

        return {
            "symbol": symbol,
            "underlying": underlying,
            "quote_currency": self.as_type(raw.get("quote_currency"), str, "USD"),
            "strike": self.as_type(raw.get("strike"), float, 0.0),
            "expiry": self.ms_to_datetime(raw.get("expiration_timestamp")),
            "option_type": option_type,
            "settlement": self.as_type(raw.get("settlement_period"), str, ""),
            "contract_size": self.as_type(raw.get("contract_size"), float, 1.0),
            "min_trade_amount": self.as_type(raw.get("min_trade_amount"), float, 0.1),
            "tick_size": self.as_type(raw.get("tick_size"), float, 0.0005),
            "is_active": raw.get("is_active", False),
            "creation_time": self.ms_to_datetime(raw.get("creation_timestamp")),
            "expiration_time": self.ms_to_datetime(raw.get("expiration_timestamp")),
            "metadata": {
                "kind": raw.get("kind", ""),
                "settlement_period": raw.get("settlement_period", ""),
                "settlement_currency": raw.get("settlement_currency", ""),
                "counter_currency": raw.get("counter_currency", ""),
                "block_trade_commission": raw.get("block_trade_commission"),
                "taker_commission": raw.get("taker_commission"),
                "maker_commission": raw.get("maker_commission"),
                "rfq": raw.get("rfq", False),
            },
        }

    def parse_instruments(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Parse list of instruments from /public/get_instruments."""
        if not raw_data:
            return []
        return [self.parse_instrument(item) for item in raw_data]

    # ==================== Book Summary / Ticker Parsing ====================

    def parse_book_summary(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse a single book summary from /public/get_book_summary_by_currency.

        Fields include: instrument_name, mark_price, bid_price, ask_price,
        volume_usd, open_interest, mark_iv, last, mid_price, etc.

        Note: mark_price for options is in BTC (fraction of underlying).
        """
        instrument_name = self.as_type(raw.get("instrument_name"), str, "")
        parsed_name = self.parse_instrument_name(instrument_name)

        greeks = raw.get("greeks", {}) or {}

        return {
            "instrument_name": instrument_name,
            "underlying": parsed_name.get("underlying", ""),
            "mark_price": self.as_type(raw.get("mark_price"), float),
            "last_price": self.as_type(raw.get("last"), float),
            "bid_price": self.as_type(raw.get("bid_price"), float),
            "ask_price": self.as_type(raw.get("ask_price"), float),
            "mid_price": self.as_type(raw.get("mid_price"), float),
            "iv": self.as_type(raw.get("mark_iv"), float),  # mark IV as percentage
            "volume_24h": self.as_type(raw.get("volume_usd"), float, 0.0),
            "open_interest": self.as_type(raw.get("open_interest"), float, 0.0),
            "underlying_price": self.as_type(raw.get("underlying_price"), float),
            "underlying_index": self.as_type(raw.get("underlying_index"), str, ""),
            # Greeks from nested dict
            "delta": self.as_type(greeks.get("delta"), float),
            "gamma": self.as_type(greeks.get("gamma"), float),
            "theta": self.as_type(greeks.get("theta"), float),
            "vega": self.as_type(greeks.get("vega"), float),
            "rho": self.as_type(greeks.get("rho"), float),
            "timestamp": self.ms_to_datetime(raw.get("creation_timestamp")),
        }

    def parse_book_summaries(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Parse list of book summaries."""
        if not raw_data:
            return []
        return [self.parse_book_summary(item) for item in raw_data]

    def parse_ticker(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse a single ticker from /public/ticker.

        This is the most detailed endpoint with full greeks.
        Fields: instrument_name, mark_price, mark_iv, best_bid_price, best_ask_price,
        greeks: {delta, gamma, theta, vega, rho}, stats: {volume_usd, ...}, etc.
        """
        instrument_name = self.as_type(raw.get("instrument_name"), str, "")
        parsed_name = self.parse_instrument_name(instrument_name)

        greeks = raw.get("greeks", {}) or {}
        stats = raw.get("stats", {}) or {}

        return {
            "instrument_name": instrument_name,
            "underlying": parsed_name.get("underlying", ""),
            "mark_price": self.as_type(raw.get("mark_price"), float),
            "last_price": self.as_type(raw.get("last_price"), float),
            "bid_price": self.as_type(raw.get("best_bid_price"), float),
            "ask_price": self.as_type(raw.get("best_ask_price"), float),
            "bid_amount": self.as_type(raw.get("best_bid_amount"), float, 0.0),
            "ask_amount": self.as_type(raw.get("best_ask_amount"), float, 0.0),
            "iv": self.as_type(raw.get("mark_iv"), float),  # percentage, e.g. 55.5
            "underlying_price": self.as_type(raw.get("underlying_price"), float),
            "underlying_index": self.as_type(raw.get("underlying_index"), str, ""),
            "settlement_price": self.as_type(raw.get("settlement_price"), float),
            # Greeks
            "delta": self.as_type(greeks.get("delta"), float),
            "gamma": self.as_type(greeks.get("gamma"), float),
            "theta": self.as_type(greeks.get("theta"), float),
            "vega": self.as_type(greeks.get("vega"), float),
            "rho": self.as_type(greeks.get("rho"), float),
            # Volume & OI
            "volume_24h": self.as_type(stats.get("volume_usd"), float, 0.0),
            "open_interest": self.as_type(raw.get("open_interest"), float, 0.0),
            # Misc
            "interest_rate": self.as_type(raw.get("interest_rate"), float),
            "estimated_delivery_price": self.as_type(raw.get("estimated_delivery_price"), float),
            "timestamp": self.ms_to_datetime(raw.get("timestamp")),
        }

    # ==================== Chart Data Parsing ====================

    def parse_chart_data(self, raw: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Parse TradingView chart data from /public/get_tradingview_chart_data.

        Response format:
        {
            "ticks": [ts1, ts2, ...],       # ms timestamps
            "open": [o1, o2, ...],
            "high": [h1, h2, ...],
            "low": [l1, l2, ...],
            "close": [c1, c2, ...],
            "volume": [v1, v2, ...],
            "cost": [c1, c2, ...],           # volume in USD
            "status": "ok"
        }
        """
        if not raw or raw.get("status") != "ok":
            return []

        ticks = raw.get("ticks", [])
        opens = raw.get("open", [])
        highs = raw.get("high", [])
        lows = raw.get("low", [])
        closes = raw.get("close", [])
        volumes = raw.get("volume", [])
        costs = raw.get("cost", [])

        n = len(ticks)
        result = []
        for i in range(n):
            result.append({
                "open_time": self.ms_to_datetime(ticks[i]),
                "open": self.as_type(opens[i] if i < len(opens) else None, float, 0.0),
                "high": self.as_type(highs[i] if i < len(highs) else None, float, 0.0),
                "low": self.as_type(lows[i] if i < len(lows) else None, float, 0.0),
                "close": self.as_type(closes[i] if i < len(closes) else None, float, 0.0),
                "volume": self.as_type(volumes[i] if i < len(volumes) else None, float, 0.0),
                "volume_usd": self.as_type(costs[i] if i < len(costs) else None, float, 0.0),
            })

        return result
