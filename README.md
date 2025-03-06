# EVE Online Market Analysis

Automated data pipeline to identify profitable trade routes between EVE Online trade hubs.

## Features

- Collects and processes market sell orders from major trade hubs (Jita, Amarr, Ren, Hek, Dodixie)
- Updates every 30 minutes using GitHub Actions
- Incorporates historical price and volume data (updated weekly)
- Identifies trade pairs with 10%+ profit margin

## Data Processing Pipeline

1. **Current Market Orders**
   - Downloads latest market orders (refreshed every 20 minutes)
   - Filters for sell orders at major trade hubs
   - Calculates supply (volume of orders within 10% of lowest price)
   - Identifies lowest priced orders per item type at each hub

2. **Historical Data**
   - Weekly collection of price and volume history
   - Used to evaluate trade stability and demand patterns

3. **Trade Pair Analysis**
   - Identifies item/station pairs with profit margins exceeding 10%
   - Ranks by profitability
   - Includes supply data to ensure sufficient trade volume

## Output

The pipeline generates `trade_analysis.csv` with the following data:
- Item type ID
- Source and destination stations
- Current lowest prices at both stations
- Available volume and supply
- Historical price and volume data
- Profit margin
- Estimated total profit potential

## Development

Requirements:
- Python 3.8+
- Dependencies listed in `requirements.txt`

To run locally:
```bash
pip install -r requirements.txt
python scripts/fetch_current_orders.py
python scripts/fetch_historic_data.py
python scripts/generate_trade_pairs.py
```

## Optimizations

1. Memory-efficient processing of large CSV files using chunking
2. Parallel processing where applicable
3. GitHub Actions caching to minimize bandwidth usage
4. Type conversion for efficient dataframe operations
5. Incremental updates to minimize processing time
