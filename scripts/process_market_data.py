#!/usr/bin/env python3
"""
Process current EVE Online market data.
Downloads and filters sell orders from specified trade hubs.
"""
import pandas as pd
import numpy as np
import requests
import bz2
import io
import os
import logging
import time
from pathlib import Path
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
MARKET_DATA_URL = "https://data.everef.net/market-orders/market-orders-latest.v3.csv.bz2"
TRADE_HUBS = {
    60003760: "Jita",
    60008494: "Amarr", 
    60004588: "Rens", 
    60005686: "Hek", 
    60011866: "Dodixie"
}
CACHE_DIR = Path("data/cache")
OUTPUT_DIR = Path("data/processed")
COLUMNS_TO_KEEP = ["price", "type_id", "volume_remain", "station_id"]
CACHE_MAX_AGE = 10  # minutes

def ensure_dirs():
    """Ensure all required directories exist."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def check_cached_data():
    """Check if we have recent cached data."""
    cache_file = CACHE_DIR / "market_data_latest.csv"
    cache_time_file = CACHE_DIR / "market_data_timestamp.txt"
    
    if cache_file.exists() and cache_time_file.exists():
        try:
            with open(cache_time_file, 'r') as f:
                cache_time = datetime.fromisoformat(f.read().strip())
            
            # If cache is fresh enough, use it
            if datetime.now() - cache_time < timedelta(minutes=CACHE_MAX_AGE):
                logger.info(f"Using cached data from {cache_time}")
                return pd.read_csv(cache_file)
        except Exception as e:
            logger.warning(f"Error reading cache: {e}")
    
    return None

def download_and_decompress():
    """Download and decompress the market data file with caching."""
    # Check cache first
    cached_data = check_cached_data()
    if cached_data is not None:
        return cached_data
    
    try:
        logger.info(f"Downloading market data from {MARKET_DATA_URL}")
        start_time = time.time()
        
        # Use streaming to handle large files
        with requests.get(MARKET_DATA_URL, stream=True) as response:
            response.raise_for_status()
            
            # Decompress data in chunks
            decompressor = bz2.BZ2Decompressor()
            chunks = []
            
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    decompressed_chunk = decompressor.decompress(chunk)
                    chunks.append(decompressed_chunk)
            
            decompressed_data = b''.join(chunks)
            
        logger.info(f"Download and decompress completed in {time.time() - start_time:.2f} seconds")
        
        # Parse CSV
        df = pd.read_csv(io.BytesIO(decompressed_data))
        logger.info(f"Loaded {len(df)} market orders")
        
        # Cache the data
        cache_file = CACHE_DIR / "market_data_latest.csv"
        cache_time_file = CACHE_DIR / "market_data_timestamp.txt"
        
        df.to_csv(cache_file, index=False)
        with open(cache_time_file, 'w') as f:
            f.write(datetime.now().isoformat())
        
        logger.info(f"Cached market data to {cache_file}")
        return df
    except Exception as e:
        logger.error(f"Error downloading or decompressing data: {e}")
        raise

def filter_and_process_data(df):
    """Filter and process market data with optimizations."""
    try:
        start_time = time.time()
        logger.info("Filtering data")
        
        # Apply filters in a memory-efficient way
        mask = (~df['is_buy_order']) & (df['station_id'].isin(TRADE_HUBS.keys()))
        filtered_df = df.loc[mask, COLUMNS_TO_KEEP]
        
        if filtered_df.empty:
            logger.error("No data after filtering, check filters or source data")
            return None
            
        logger.info(f"Filtered to {len(filtered_df)} sell orders in trade hubs")
        
        # Use more efficient methods for grouping
        # Convert to categorical for better performance
        filtered_df['type_id'] = filtered_df['type_id'].astype('category')
        filtered_df['station_id'] = filtered_df['station_id'].astype('category')
        
        # Count occurrences of each type_id across stations more efficiently
        type_counts = filtered_df.groupby('type_id')['station_id'].nunique()
        valid_types = type_counts[type_counts > 1].index
        filtered_df = filtered_df[filtered_df['type_id'].isin(valid_types)]
        logger.info(f"Keeping {len(valid_types)} item types that appear in multiple hubs")
        
        # Calculate supply (sum of volumes within 10% of lowest price per type per station)
        logger.info("Calculating supply values")
        supply_df = calculate_supply(filtered_df)
        
        # Keep only lowest price row per type_id per station
        logger.info("Finding lowest prices")
        result_df = (filtered_df.sort_values('price')
                    .groupby(['type_id', 'station_id'])
                    .first()
                    .reset_index())
        
        # Merge with supply data
        result_df = pd.merge(
            result_df, 
            supply_df[['type_id', 'station_id', 'supply']], 
            on=['type_id', 'station_id']
        )
        
        logger.info(f"Processing completed in {time.time() - start_time:.2f} seconds")
        return result_df
    except Exception as e:
        logger.error(f"Error processing data: {e}")
        raise

def calculate_supply(df):
    """Calculate supply more efficiently using vectorized operations where possible."""
    try:
        start_time = time.time()
        supply_data = []
        
        # Pre-sort dataframe once to avoid repeated sorting in the loop
        df = df.sort_values(['type_id', 'station_id', 'price'])
        
        # Group by type_id and station_id
        for (type_id, station_id), group in df.groupby(['type_id', 'station_id']):
            # Already sorted by price
            lowest_price = group['price'].iloc[0]
            threshold = lowest_price * 1.1
            
            # Use vectorized operations
            eligible_orders = group[group['price'] <= threshold]
            supply = eligible_orders['volume_remain'].sum()
            
            supply_data.append({
                'type_id': type_id,
                'station_id': station_id,
                'supply': supply
            })
        
        result = pd.DataFrame(supply_data)
        logger.info(f"Supply calculation completed in {time.time() - start_time:.2f} seconds")
        return result
    except Exception as e:
        logger.error(f"Error calculating supply: {e}")
        raise

def main():
    """Main function with better error handling and timing."""
    overall_start = time.time()
    try:
        ensure_dirs()
        df = download_and_decompress()
        processed_df = filter_and_process_data(df)
        
        if processed_df is not None:
            # Save processed data
            output_path = OUTPUT_DIR / "current_market_data.csv"
            processed_df.to_csv(output_path, index=False)
            logger.info(f"Saved processed data to {output_path} with {len(processed_df)} rows")
        
        logger.info(f"Total processing time: {time.time() - overall_start:.2f} seconds")
    except Exception as e:
        logger.error(f"Error in main processing: {e}")
        raise

if __name__ == "__main__":
    main()
