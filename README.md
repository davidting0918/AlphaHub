# AlphaHub

A collection of crypto trading strategies, analysis tools, and alpha discovery projects.

## Structure

```
AlphaHub/
├── shared/              # Shared utilities & exchange adaptors
│   └── adaptor/         # Exchange API clients (Binance, OKX, etc.)
├── projects/            # Individual trading projects
│   └── bn_alpha_monitor # Binance Alpha stability monitor
├── scripts/             # One-off debug & test scripts
└── requirements.txt
```

## Projects

| Project | Description | Status |
|---------|-------------|--------|
| [bn_alpha_monitor](projects/bn_alpha_monitor/) | Binance Alpha token stability monitor | ✅ Active |

## Setup

```bash
pip install -r requirements.txt
```

## Adding a New Project

1. Create a folder under `projects/`
2. Add a `README.md` describing the strategy
3. Import shared utilities from `shared/` as needed
