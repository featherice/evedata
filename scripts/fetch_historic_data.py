#!/usr/bin/env python3
"""
Fetch historical price and volume data for EVE Online trade hubs.
"""

import os
import sys
import requests
import pandas as pd
from datetime import datetime, timedelta
import logging
from tqdm import tqdm

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('fetch_historic.log')
    ]
)
logger = logging.getLogger('fetch_historic')

# Constants
OUTPUT_DIR = "data/raw"
TARGET_HUBS = [60003760, 60008494, 60004588, 60005686, 60011866]  # Jita, Amarr, Ren, Hek, Dodixie
BASE_URL_PRICES = "https://static.adam4eve.eu/MarketPricesStationHistory"
BASE_URL_VOLUMES = "https://static.adam4eve.eu/MarketVolumesStationHistory"

def ensure_directories():
    """Ensure required directories exist."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)

def get_current_week_file():
    """Determine the current week file based on the date."""
    now = datetime.now()
    year = now.year
    
    # Get the ISO week number (1-53)
    week = now.isocalendar()[1]
    
    # Check if we need to look at previous week (if it's Monday and data not yet updated)
    if now.weekday() == 0 and now.hour < 12:  # Monday before noon
        # Go back one week
        prev_date = now - timedelta(days=7)
        year = prev_date.year
        week = prev_date.isocalendar()[1]
    
    # Format week number with leading zero if needed
    week_str = f"{week:02d}"
    
    return year, f"{year}-{week_str}"

def download_historic_data(data_type):
    """Download historical data (prices or volumes)."""
    year, week_str = get_current_week_file()
    
    if data_type == "prices":
        base_url = BASE_URL_PRICES
        file_prefix = "MarketPricesStationHistory"
    else:
        base_url = BASE_URL_VOLUMES
        file_prefix = "MarketVolumesStationHistory"
    
    url = f"{base_url}/{year}/{file_prefix}_hub_weekly_{week_str}.csv"
    output_file = os.path.join(OUTPUT_DIR, f"historic_{data_type}.csv")
    
    try:
        logger.info(f"Downloading {data_type} data from {url}")
        response = requests.get(url)
        response.raise_for_status()
        
        # Parse CSV data with semicolon separator
        df = pd.read_csv(io.StringIO(response.text), sep=';')
        
        # Filter for target hubs
        df = df[df['location_id'].isin(TARGET_HUBS)]
        
        # Save to file
        df.to_csv(output_file, index=False)
        logger.info(f"Saved {data_type} data to {output_file}")
        
        return df
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to download {data_type} data: {e}")
        
        # Check if we need to try the previous week
        if "404" in str(e):
            logger.info("Trying previous week's data")
            prev_date = datetime.now() - timedelta(days=7)
            prev_year = prev_date.year
            prev_week = f"{prev_date.isocalendar()[1]:02d}"
            
            url = f"{base_url}/{prev_year}/{file_prefix}_hub_weekly_{prev_year}-{prev_week}.csv"
            
            try:
                logger.info(f"Downloading {data_type} data from {url}")
                response = requests.get(url)
                response.raise_for_status()
                
                df = pd.read_csv(io.StringIO(response.text), sep=';')
                df = df[df['location_id'].isin(TARGET_HUBS)]
                df.to_csv(output_file, index=False)
                logger.info(f"Saved {data_type} data to {output_file}")
                
                return df
            except requests.exceptions.RequestException as e2:
                logger.error(f"Failed to download previous week's {data_type} data: {e2}")
                raise
        else:
            raise

def main():
    """Main function to download historical price and volume data."""
    try:
        ensure_directories()
        
        # Download price data
        price_data = download_historic_data("prices")
        
        # Download volume data
        volume_data = download_historic_data("volumes")
        
        logger.info("Historical data download complete")
        
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
