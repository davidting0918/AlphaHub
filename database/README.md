# AlphaHub Database

Shared database schema for all trading strategies.

## Entity Relationship

```
Strategy
  └── Portfolio (account running this strategy)
        ├── Orders (execution records)
        ├── Positions (open & closed)
        ├── PnL Snapshots (periodic performance)
        ├── Balance Snapshots (account balance)
        ├── Funding Payments (funding arb specific)
        └── Strategy Logs (signals, decisions, errors)

Exchange
  ├── Portfolio (which exchange this portfolio uses)
  ├── Instruments (tradeable pairs on this exchange)
  └── Funding Rates (market data, shared across portfolios)
```

## Key Concepts

### Strategy
The trading algorithm / logic definition. e.g. "Funding Rate Arbitrage"

### Portfolio
An instance of a strategy running on a specific exchange account. One strategy can have multiple portfolios (different accounts, different capital).

### Position Group
A UUID that groups related positions/orders together. For example, in funding rate arb, a spot long + perp short opened together share the same `position_group_id`.

## Setup

```bash
# Apply schema to your database
psql $DATABASE_URL -f schema.sql
```

## Adding New Strategies

1. Add a row to `strategies` table
2. If the strategy needs custom data tables, add them in a new section (like `funding_rates` / `funding_payments` for funding arb)
3. All execution tables (`orders`, `positions`, `pnl_snapshots`) are shared — no changes needed
