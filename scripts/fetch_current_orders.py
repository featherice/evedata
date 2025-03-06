#!/usr/bin/env python3
"""
Fetch and process current market orders from EVE Online.
"""

import os
import sys
import requests
import pandas as pd
import bz2
import io
from tqdm import tqdm
import logging
from concurrent.futures import ThreadPoolExecutor
import numpy as np

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('fetch_orders.log')
    ]
)
logger = logging.getLogger('fetch_orders')

# Constants
MARKET_ORDERS_URL = "https://data.everef.net/market-orders/market-orders-latest.v3.csv.bz2"
OUTPUT_DIR = "data/raw"
PROCESSED_DIR = "data/processed"
TARGET_HUBS = [60003760, 60008494, 60004588, 60005686, 60011866]  # Jita, Amarr, Ren, Hek, Dodixie
COLUMNS_TO_KEEP = ['price', 'type_id', 'volume_remain', 'station_id']
PROFIT_MARGIN_THRESHOLD = 0.1  # 10%

def ensure_directories():
    """Ensure required directories exist."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(PROCESSED_DIR, exist_ok=True)

def download_file(url):
    """Download file with progress bar."""
    try:
        logger.info(f"Downloading data from {url}")
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        total_size = int(response.headers.get('content-length', 0))
        block_size = 8192
        
        # Use BytesIO to avoid writing compressed file to disk
        buffer = io.BytesIO()
        with tqdm(total=total_size, unit='B', unit_scale=True, desc="Downloading") as pbar:
            for chunk in response.iter_content(chunk_size=block_size):
                if chunk:
                    buffer.write(chunk)
                    pbar.update(len(chunk))
        
        buffer.seek(0)
        return buffer
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to download file: {e}")
        raise

def decompress_and_filter_data(compressed_data):
    """Decompress bz2 data and filter for sell orders in target hubs."""
    try:
        logger.info("Decompressing and filtering data")
        
        # Decompress data
        with bz2.open(compressed_data) as f:
            # Use chunking to handle large files efficiently
            chunks = []
            for chunk in pd.read_csv(f, chunksize=100000):
                # Filter for sell orders (is_buy_order=false) in target hubs
                filtered_chunk = chunk[(~chunk['is_buy_order']) & 
                                      (chunk['station_id'].isin(TARGET_HUBS))]
                
                # Keep only needed columns
                filtered_chunk = filtered_chunk[COLUMNS_TO_KEEP]
                chunks.append(filtered_chunk)
            
            df = pd.concat(chunks, ignore_index=True)
            
        logger.info(f"Filtered to {len(df)} sell orders in target hubs")
        return df
    except Exception as e:
        logger.error(f"Error during decompression and filtering: {e}")
        raise

def process_orders(df):
    """Process orders to calculate supply and find lowest prices."""
    try:
        logger.info("Processing orders")
        
        # Convert type_id and station_id to integers for efficient operations
        df['type_id'] = df['type_id'].astype(np.int32)
        df['station_id'] = df['station_id'].astype(np.int32)
        df['price'] = df['price'].astype(np.float32)
        df['volume_remain'] = df['volume_remain'].astype(np.int32)
        
        # Step 1: Discard rows for type_ids present only at a single hub
        type_id_hub_counts = df.groupby('type_id')['station_id'].nunique()
        valid_type_ids = type_id_hub_counts[type_id_hub_counts > 1].index
        df = df[df['type_id'].isin(valid_type_ids)]
        
        if df.empty:
            logger.warning("No items found that exist in multiple hubs")
            return pd.DataFrame()
        
        # Step 2: Calculate supply for each type_id at each station
        # Find orders within 10% of lowest price for each type_id/station_id combination
        df_grouped = df.groupby(['type_id', 'station_id'])
        
        supply_data = []
        min_price_orders = []
        
        for (type_id, station_id), group in tqdm(df_grouped, desc="Processing groups"):
            min_price = group['price'].min()
            price_threshold = min_price * 1.1  # 10% above minimum
            
            # Filter orders within threshold
            within_threshold = group[group['price'] <= price_threshold]
            supply = within_threshold['volume_remain'].sum()
            
            # Get the lowest price order
            min_price_order = group.loc[group['price'].idxmin()].copy()
            min_price_order['supply'] = supply
            
            min_price_orders.append(min_price_order)
        
        # Create result dataframe with lowest price orders and supply
        result_df = pd.DataFrame(min_price_orders)
        
        logger.info(f"Processed {len(result_df)} unique type_id/station combinations")
        return result_df
        
    except Exception as e:
        logger.error(f"Error during order processing: {e}")
        raise

def main():
    """Main function to download and process EVE market orders."""
    try:
        ensure_directories()
        
        # Download compressed data
        compressed_data = download_file(MARKET_ORDERS_URL)
        
        # Process the data
        filtered_df = decompress_and_filter_data(compressed_data)
        processed_df = process_orders(filtered_df)
        
        if processed_df.empty:
            logger.warning("No data to save after processing")
            return
        
        # Save processed data
        output_path = os.path.join(OUTPUT_DIR, "current_orders.csv")
        processed_df.to_csv(output_path, index=False)
        logger.info(f"Saved processed data to {output_path}")
        
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
