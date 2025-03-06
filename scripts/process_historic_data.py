#!/usr/bin/env python3
"""
Process historic EVE Online market data.
Downloads and processes weekly historic price and volume data.
"""
import pandas as pd
import requests
import logging
from pathlib import Path
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
TRADE_HUBS = {
    60003760: "Jita",
    60008494: "Amarr", 
    60004588: "Rens", 
    60005686: "Hek", 
    60011866: "Dodixie"
}
OUTPUT_DIR = Path("data/processed")
CACHE_DIR = Path("data/cache")

def ensure_dirs():
    """Ensure all required directories exist."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def get_current_week_file_paths():
    """Get current week's file paths for historic data."""
    try:
        now = datetime.now()
        year = now.year
        
        # Week number with leading zero if needed
        week_num = now.strftime("%U")
        if week_num.startswith('0'):
            week_num = week_num[1:]
            
        price_url = f"https://static.adam4eve.eu/MarketPricesStationHistory/{year}/MarketPricesStationHistory_hub_weekly_{year}-{week_num}.csv"
        volume_url = f"https://static.adam4eve.eu/MarketVolumesStationHistory/{year}/MarketVolumesStationHistory_hub_weekly_{year}-{week_num}.csv"
        
        logger.info(f"Using files for year {year}, week {week_num}")
        return price_url, volume_url
    except Exception as e:
        logger.error(f"Error determining file paths: {e}")
        raise

def download_csv(url):
    """Download a CSV file from a URL."""
    try:
        logger.info(f"Downloading {url}")
        response = requests.get(url)
        response.raise_for_status()
        
        # Parse as CSV with semicolon delimiter
        df = pd.read_csv(io.StringIO(response.text), delimiter=';')
        logger.info(f"Downloaded {len(df)} rows")
        return df
    except Exception as e:
        logger.error(f"Error downloading file {url}: {e}")
        raise

def process_historic_data():
    """Process historic price and volume data."""
    try:
        price_url, volume_url = get_current_week_file_paths()
        
        # Download data
        price_df = download_csv(price_url)
        volume_df = download_csv(volume_url)
        
        # Filter for our trade hubs
        price_df = price_df[price_df['location_id'].isin(TRADE_HUBS.keys())]
        volume_df = volume_df[volume_df['location_id'].isin(TRADE_HUBS.keys())]
        
        # Select relevant columns
        price_cols = ['type_id', 'location_id', 'sell_price_low', 'sell_price_avg', 'sell_price_high']
        volume_cols = ['type_id', 'location_id', 'sell_volume_low', 'sell_volume_avg', 'sell_volume_high']
        
        price_df = price_df[price_cols]
        volume_df = volume_df[volume_cols]
        
        # Rename location_id to station_id for consistency
        price_df = price_df.rename(columns={'location_id': 'station_id'})
        volume_df = volume_df.rename(columns={'location_id': 'station_id'})
        
        # Merge price and volume data
        historic_df = pd.merge(
            price_df,
            volume_df,
            on=['type_id', 'station_id']
        )
        
        # Save to file
        output_path = OUTPUT_DIR / "historic_market_data.csv"
        historic_df.to_csv(output_path, index=False)
        logger.info(f"Saved historic data to {output_path} with {len(historic_df)} rows")
        
        return historic_df
    except Exception as e:
        logger.error(f"Error processing historic data: {e}")
        raise

def main():
    """Main function to process historic data."""
    try:
        ensure_dirs()
        process_historic_data()
    except Exception as e:
        logger.error(f"Error in main processing: {e}")
        raise

if __name__ == "__main__":
    main()
