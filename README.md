# EVE Online Market Data Processing

This repository contains scripts to process EVE Online market data and find profitable trade opportunities between major trade hubs.

## Features

- Automatic market data processing every 30 minutes
- Weekly historic data updates
- Trade pair analysis with profitability calculation

## Trade Hubs

- Jita (60003760)
- Amarr (60008494)
- Rens (60004588)
- Hek (60005686)
- Dodixie (60011866)

## Data Sources

- Current market orders: https://data.everef.net/market-orders/market-orders-latest.v3.csv.bz2
- Historic price data: https://static.adam4eve.eu/MarketPricesStationHistory/
- Historic volume data: https://static.adam4eve.eu/MarketVolumesStationHistory/

## Setup

1. Clone this repository
2. GitHub Actions is configured to run automatically
3. For local testing:
   ```bash
   pip install pandas numpy requests bz2file
   python scripts/process_market_data.py
   python scripts/process_historic_data.py
   python scripts/generate_trade_pairs.py
   ```

## Output Files

- `data/processed/current_market_data.csv` - Current market data
- `data/processed/historic_market_data.csv` - Historic market data (updated weekly)
- `data/results/trade_pairs.csv` - All profitable trade pairs

## Error Handling

All scripts include error handling and logging. Check GitHub Actions logs for details if issues occur.
