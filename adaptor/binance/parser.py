"""
Binance Parser - Data Parsing Layer

This module provides pure data parsing functions to convert raw Binance API responses
into standardized formats matching our database schema.
"""

from typing import Any, Dict, List, Optional, Type
from datetime import datetime, timezone


class BinanceParser:
    """Parser for Binance API responses (Futures + Alpha)"""

    @staticmethod
    def as_type(value: Any, to_type: Type[Any], default: Any = None) -> Any:
        """Safely convert value to target type"""
        if value is None or value == '':
            return default if default is not None else (to_type() if to_type != float else 0.0)
        try:
            return to_type(value)
        except (ValueError, TypeError):
            return default if default is not None else (to_type() if to_type != float else 0.0)

    @staticmethod
    def ms_to_datetime(ms_timestamp: Any) -> Optional[datetime]:
        """Convert millisecond timestamp to datetime"""
        if ms_timestamp is None or ms_timestamp == '' or ms_timestamp == 0:
            return None
        try:
            ts = int(ms_timestamp) / 1000.0
            return datetime.fromtimestamp(ts, tz=timezone.utc)
        except (ValueError, TypeError, OSError):
            return None

    def _get_filter_value(self, filters: List[Dict], filter_type: str, key: str) -> Optional[str]:
        """Extract a value from Binance symbol filters."""
        for f in filters:
            if f.get('filterType') == filter_type:
                return f.get(key)
        return None

    def parse_perp_instrument(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse a PERPETUAL symbol from Binance Futures exchangeInfo.

        Binance fields mapping:
        - symbol: "BTCUSDT"
        - pair: "BTCUSDT"
        - contractType: "PERPETUAL"
        - baseAsset: "BTC"
        - quoteAsset: "USDT"
        - marginAsset: "USDT"  (settlement currency)
        - status: "TRADING" → is_active
        - onboardDate: listing timestamp (ms)
        - filters: contains PRICE_FILTER (tickSize), LOT_SIZE (minQty, stepSize)
        """
        symbol = self.as_type(raw.get('symbol'), str, '')
        base = self.as_type(raw.get('baseAsset'), str, '')
        quote = self.as_type(raw.get('quoteAsset'), str, '')
        margin_asset = self.as_type(raw.get('marginAsset'), str, quote)

        filters = raw.get('filters', [])
        tick_size = self._get_filter_value(filters, 'PRICE_FILTER', 'tickSize')
        min_qty = self._get_filter_value(filters, 'LOT_SIZE', 'minQty')
        step_size = self._get_filter_value(filters, 'LOT_SIZE', 'stepSize')
        min_notional = self._get_filter_value(filters, 'MIN_NOTIONAL', 'notional')

        instrument_id = f"binance_PERP_{base}_{quote}"

        return {
            'instrument_id': instrument_id,
            'symbol': symbol,
            'type': 'PERP',
            'base_currency': base,
            'quote_currency': quote,
            'settle_currency': margin_asset,
            'contract_size': 1.0,  # Binance USDT-M futures: 1 contract = 1 unit of base asset
            'multiplier': 1,
            'min_size': self.as_type(min_qty, float, 0.0),
            'is_active': raw.get('status') == 'TRADING',
            'listing_time': self.ms_to_datetime(raw.get('onboardDate')),
            'metadata': {
                'tick_size': tick_size or '',
                'step_size': step_size or '',
                'min_notional': min_notional or '',
                'price_precision': raw.get('pricePrecision'),
                'quantity_precision': raw.get('quantityPrecision'),
                'underlying_type': self.as_type(raw.get('underlyingType'), str, ''),
            }
        }

    def parse_instruments(self, raw_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Parse exchangeInfo response, filtering for PERPETUAL contracts only.

        Args:
            raw_data: Raw /fapi/v1/exchangeInfo response

        Returns:
            List of standardized instrument dicts
        """
        symbols = raw_data.get('symbols', [])
        if not symbols:
            return []

        return [
            self.parse_perp_instrument(s)
            for s in symbols
            if s.get('contractType') == 'PERPETUAL'
        ]

    # ==================== Funding Rate Parsing ====================

    def parse_funding_rate(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse a single Binance funding rate record.

        Binance fields:
        - symbol: "BTCUSDT"
        - fundingRate: rate as decimal string
        - fundingTime: settlement timestamp (ms)
        - markPrice: mark price at funding time
        """
        return {
            'inst_id': self.as_type(raw.get('symbol'), str, ''),
            'funding_rate': self.as_type(raw.get('fundingRate'), float, 0.0),
            'funding_time': self.ms_to_datetime(raw.get('fundingTime')),
            'realized_rate': self.as_type(raw.get('fundingRate'), float, 0.0),  # Binance: fundingRate IS the realized rate
            'next_funding_rate': None,  # Not available in history endpoint
            'next_funding_time': None,
            'metadata': {
                'mark_price': self.as_type(raw.get('markPrice'), str, ''),
            }
        }

    def parse_funding_rates(self, raw_data) -> List[Dict[str, Any]]:
        """
        Parse funding rates response.

        Args:
            raw_data: List of funding rate records (Binance returns a list directly)
        """
        if not raw_data:
            return []
        return [self.parse_funding_rate(item) for item in raw_data]

    # ==================== Futures Kline Parsing ====================

    def parse_futures_kline(self, raw: List[Any]) -> Dict[str, Any]:
        """
        Parse a single Binance Futures kline.

        Binance kline array format:
        [openTime, open, high, low, close, volume, closeTime,
         quoteAssetVolume, numberOfTrades, takerBuyBaseVol, takerBuyQuoteVol, ignore]
        """
        return {
            'open_time': self.ms_to_datetime(raw[0]),
            'open': self.as_type(raw[1], float, 0.0),
            'high': self.as_type(raw[2], float, 0.0),
            'low': self.as_type(raw[3], float, 0.0),
            'close': self.as_type(raw[4], float, 0.0),
            'volume': self.as_type(raw[5], float, 0.0),
            'close_time': self.ms_to_datetime(raw[6]),
            'quote_volume': self.as_type(raw[7], float, 0.0),
            'trade_count': self.as_type(raw[8], int, 0),
        }

    def parse_futures_klines(self, raw_data: List[List[Any]]) -> List[Dict[str, Any]]:
        """Parse Binance Futures klines response (returns list of arrays)."""
        if not raw_data:
            return []
        return [self.parse_futures_kline(item) for item in raw_data]

    # ==================== Alpha Parsing ====================

    def parse_token_list(self, raw_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        if not raw_data.get('data'):
            return []
        return [
            {
                'alpha_id': self.as_type(row['alphaId'], str),
                'token_id': self.as_type(row['tokenId'], str),
                'symbol': self.as_type(row['symbol'], str),
                'volume_24h': self.as_type(row['volume24h'], float),
                'market_cap': self.as_type(row['marketCap'], float),
                'high_price_24h': self.as_type(row['priceHigh24h'], float),
                'low_price_24h': self.as_type(row['priceLow24h'], float),
                'trade_count_24h': self.as_type(row['count24h'], int),
                'holder_count': self.as_type(row['holders'], int),
                'listing_timestamp': self.as_type(row['listingTime'], int),
                'multiplier': self.as_type(row['mulPoint'], int),
            }
            for row in raw_data['data']
        ]

    def parse_klines(self, raw_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        if not raw_data.get('data'):
            return []
        return [
            {
                "timestamp": self.as_type(row[0], int),
                "open": self.as_type(row[1], float),
                "high": self.as_type(row[2], float),
                "low": self.as_type(row[3], float),
                "close": self.as_type(row[4], float),
                "volume": self.as_type(row[5], float),
                "end_timestamp": self.as_type(row[6], int),
                "quote_volume": self.as_type(row[7], float),
                "trade_count": self.as_type(row[8], int),
                "taker_base_volume": self.as_type(row[9], float),
                "taker_quote_volume": self.as_type(row[10], float),
            }
            for row in raw_data['data']
        ]

    def parse_agg_trades(self, raw_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        if not raw_data.get('data'):
            return []
        return [
            {
                "timestamp": self.as_type(row['T'], int),
                "trade_id": self.as_type(row['a'], str),
                "price": self.as_type(row['p'], float),
                "qty": self.as_type(row['q'], float),
            }
            for row in raw_data['data']
        ]


