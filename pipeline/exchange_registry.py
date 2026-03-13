"""
Exchange Registry

Maps exchange_id / exchange_name → Exchange Client instance.
Each adaptor client must set EXCHANGE_ID to match the DB.
"""

import logging
from typing import Any

from adaptor.okx.client import OKXClient

logger = logging.getLogger(__name__)

# Registry: exchange_name (uppercase) → client class
_EXCHANGE_CLIENTS = {
    "OKX": OKXClient,
    "OKXTEST": OKXClient,       # same client, same public API
    "BINANCE": None,            # TODO: implement BinanceClient for pipeline
}


def get_exchange_client(exchange_id: int, exchange_name: str) -> Any:
    """
    Get an exchange client instance based on exchange_id and name.

    Args:
        exchange_id: DB id from exchanges table
        exchange_name: Name from exchanges table (e.g. 'OKX', 'BINANCE')

    Returns:
        Exchange client instance with .get_instruments(), .get_funding_rate_history() etc.

    Raises:
        ValueError: If exchange not supported
    """
    name_upper = exchange_name.upper()
    client_class = _EXCHANGE_CLIENTS.get(name_upper)

    if client_class is None:
        raise ValueError(
            f"Exchange '{exchange_name}' (id={exchange_id}) not supported. "
            f"Available: {list(_EXCHANGE_CLIENTS.keys())}"
        )

    client = client_class()
    logger.info(f"Created {client_class.__name__} for exchange '{exchange_name}' (id={exchange_id})")
    return client
