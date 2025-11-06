"""
Binance Alpha Parser - Data Parsing Layer

This module provides pure data parsing functions to convert raw API responses
into structured pandas DataFrames. All functions are synchronous and stateless.
"""

from typing import Any, Type

class BinanceAlphaParser:

    @staticmethod
    def as_type(value: Any, to_type: Type[Any]) -> Any:
        if not value:
            return to_type()
        return to_type(value)

    def parse_token_list(self, raw_data):
        if not raw_data['data']:
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
                'multiplier': self.as_type(row['mulPoint'], int)
            } for row in raw_data['data']
        ]

    def parse_klines(self, raw_data):
        if not raw_data['data']:
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

    def parse_agg_trades(self, raw_data):
        if not raw_data['data']:
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