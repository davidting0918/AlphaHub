"""
OKX Parser - Data Parsing Layer

This module provides pure data parsing functions to convert raw OKX API responses
into standardized formats matching our database schema.
"""

from typing import Any, Dict, List, Optional, Type
from datetime import datetime, timezone


class OKXParser:
    """Parser for OKX API responses"""
    
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
        if ms_timestamp is None or ms_timestamp == '':
            return None
        try:
            ts = int(ms_timestamp) / 1000.0
            return datetime.fromtimestamp(ts, tz=timezone.utc)
        except (ValueError, TypeError, OSError):
            return None
    
    def parse_swap_instrument(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse a SWAP (perpetual) instrument from OKX response
        
        OKX fields mapping:
        - instId: "BTC-USDT-SWAP" → symbol, base/quote extracted
        - ctVal: contract value (e.g., 0.01 BTC per contract)
        - ctMult: contract multiplier
        - minSz: minimum order size
        - tickSz: tick size → stored in metadata
        - settleCcy: settlement currency
        - state: "live" → is_active
        - listTime: listing timestamp
        
        Returns standardized dict for instruments table
        """
        inst_id = self.as_type(raw.get('instId'), str, '')
        
        # Parse base and quote from instId: "BTC-USDT-SWAP" → base=BTC, quote=USDT
        parts = inst_id.split('-')
        base = parts[0] if len(parts) >= 1 else ''
        quote = parts[1] if len(parts) >= 2 else ''
        
        # Standardized instrument_id format
        instrument_id = f"okx_PERP_{base}_{quote}"
        
        return {
            'instrument_id': instrument_id,
            'symbol': inst_id,
            'type': 'PERP',
            'base_currency': base,
            'quote_currency': quote,
            'settle_currency': self.as_type(raw.get('settleCcy'), str, quote),
            'contract_size': self.as_type(raw.get('ctVal'), float, 1.0),
            'multiplier': self.as_type(raw.get('ctMult'), int, 1),
            'min_size': self.as_type(raw.get('minSz'), float, 0.0),
            'is_active': raw.get('state') == 'live',
            'listing_time': self.ms_to_datetime(raw.get('listTime')),
            'metadata': {
                'tick_size': self.as_type(raw.get('tickSz'), str, ''),
                'lot_size': self.as_type(raw.get('lotSz'), str, ''),
                'ct_type': self.as_type(raw.get('ctType'), str, ''),  # linear/inverse
                'lever': self.as_type(raw.get('lever'), str, ''),
            }
        }
    
    def parse_spot_instrument(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse a SPOT instrument from OKX response
        
        OKX fields mapping:
        - instId: "BTC-USDT"
        - baseCcy: base currency
        - quoteCcy: quote currency
        - minSz: minimum order size
        - tickSz: tick size → stored in metadata
        - state: "live" → is_active
        - listTime: listing timestamp
        
        Returns standardized dict for instruments table
        """
        inst_id = self.as_type(raw.get('instId'), str, '')
        base = self.as_type(raw.get('baseCcy'), str, '')
        quote = self.as_type(raw.get('quoteCcy'), str, '')
        
        # Standardized instrument_id format
        instrument_id = f"okx_SPOT_{base}_{quote}"
        
        return {
            'instrument_id': instrument_id,
            'symbol': inst_id,
            'type': 'SPOT',
            'base_currency': base,
            'quote_currency': quote,
            'settle_currency': quote,  # SPOT settles in quote currency
            'contract_size': None,  # N/A for spot
            'multiplier': 1,
            'min_size': self.as_type(raw.get('minSz'), float, 0.0),
            'is_active': raw.get('state') == 'live',
            'listing_time': self.ms_to_datetime(raw.get('listTime')),
            'metadata': {
                'tick_size': self.as_type(raw.get('tickSz'), str, ''),
                'lot_size': self.as_type(raw.get('lotSz'), str, ''),
            }
        }
    
    def parse_instruments(self, raw_data: Dict[str, Any], inst_type: str = "SWAP") -> List[Dict[str, Any]]:
        """
        Parse instruments response from OKX
        
        Args:
            raw_data: Raw API response
            inst_type: "SWAP" or "SPOT"
            
        Returns:
            List of standardized instrument dicts
        """
        data = raw_data.get('data', [])
        if not data:
            return []
        
        if inst_type == "SWAP":
            return [self.parse_swap_instrument(item) for item in data]
        elif inst_type == "SPOT":
            return [self.parse_spot_instrument(item) for item in data]
        else:
            return []
    
    def parse_funding_rate(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse a single funding rate record
        
        OKX fields mapping:
        - fundingRate: current/realized funding rate (as decimal, e.g., 0.0001 = 0.01%)
        - fundingTime: settlement timestamp (ms)
        - realizedRate: actual settled rate (for historical)
        - nextFundingRate: predicted next rate
        - nextFundingTime: next settlement time
        
        Returns standardized dict for funding_rates table
        """
        return {
            'inst_id': self.as_type(raw.get('instId'), str, ''),
            'funding_rate': self.as_type(raw.get('fundingRate'), float, 0.0),
            'funding_time': self.ms_to_datetime(raw.get('fundingTime')),
            'realized_rate': self.as_type(raw.get('realizedRate'), float),
            'next_funding_rate': self.as_type(raw.get('nextFundingRate'), float),
            'next_funding_time': self.ms_to_datetime(raw.get('nextFundingTime')),
        }
    
    def parse_funding_rates(self, raw_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Parse funding rates response (current or historical)
        
        Args:
            raw_data: Raw API response
            
        Returns:
            List of standardized funding rate dicts
        """
        data = raw_data.get('data', [])
        if not data:
            return []
        
        return [self.parse_funding_rate(item) for item in data]

    # ==================== Kline Parsing ====================

    def parse_kline(self, raw: List[Any]) -> Dict[str, Any]:
        """
        Parse a single OKX candlestick.

        OKX kline array format:
        [ts, o, h, l, c, vol, volCcy, volCcyQuote, confirm]
        - ts: opening time in ms
        - confirm: "0" = incomplete, "1" = complete
        """
        return {
            'open_time': self.ms_to_datetime(raw[0]),
            'open': self.as_type(raw[1], float, 0.0),
            'high': self.as_type(raw[2], float, 0.0),
            'low': self.as_type(raw[3], float, 0.0),
            'close': self.as_type(raw[4], float, 0.0),
            'volume': self.as_type(raw[5], float, 0.0),
            'quote_volume': self.as_type(raw[7], float, 0.0) if len(raw) > 7 else 0.0,
            'confirm': raw[8] == '1' if len(raw) > 8 else True,
        }

    def parse_klines(self, raw_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Parse klines/candlestick response from OKX."""
        data = raw_data.get('data', [])
        if not data:
            return []
        return [self.parse_kline(item) for item in data]
