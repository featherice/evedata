#!/usr/bin/env python3
"""
Generate trade pair analysis for EVE Online markets.
"""

import os
import sys
import pandas as pd
import numpy as np
from itertools import permutations
import logging
from tqdm import tqdm

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('trade_pairs.log')
    ]
)
logger = logging.getLogger('trade_pairs')

# Constants
RAW_DIR = "data/raw"
PROCESSED_DIR = "data/processed"
PROFIT_MARGIN_THRESHOLD = 0.1  # 10%

def ensure_directories():
    """Ensure required directories exist."""
    os.makedirs(PROCESSED_DIR, exist_ok=True)

def load_current_orders():
    """Load current market orders."""
    try:
        file_path = os.path.join(RAW_DIR, "current_orders.csv")
        if not os.path.exists(file_path):
            logger.error(f"Current orders file not found: {file_path}")
            return None
            
        df = pd.read_csv(file_path)
        logger.info(f"Loaded {len(df)} current market orders")
        return df
    except Exception as e:
        logger.error(f"Error loading current orders: {e}")
        return None

def load_historic_data():
    """Load historical price and volume data."""
    try:
        prices_path = os.path.join(RAW_DIR, "historic_prices.csv")
        volumes_path = os.path.join(RAW_DIR, "historic_volumes.csv")
        
        if not os.path.exists(prices_path) or not os.path.exists(volumes_path):
            logger.warning("Historical data files not found")
            return None, None
            
        prices_df = pd.read_csv(prices_path)
        volumes_df = pd.read_csv(volumes_path)
        
        logger.info(f"Loaded {len(prices_df)} historic price records and {len(volumes_df)} volume records")
        return prices_df, volumes_df
    except Exception as e:
        logger.error(f"Error loading historical data: {e}")
        return None, None

def generate_trade_pairs(orders_df, prices_df=None, volumes_df=None):
    """Generate trade pairs with profit potential."""
    try:
        if orders_df is None or len(orders_df) == 0:
            logger.error("No order data available")
            return None
            
        # Convert data types for efficiency
        orders_df['type_id'] = orders_df['type_id'].astype(np.int32)
        orders_df['station_id'] = orders_df['station_id'].astype(np.int32)
        
        # Create list to hold trade pairs
        trade_pairs = []
        
        # Group orders by type_id
        grouped = orders_df.groupby('type_id')
        
        # For each item type
        for type_id, item_group in tqdm(grouped, desc="Processing items"):
            # Skip if less than 2 stations have this item
            if len(item_group['station_id'].unique()) < 2:
                continue
                
            # Generate all possible station pairs (source → destination)
            for start_station, dest_station in permutations(item_group['station_id'].unique(), 2):
                # Get orders for each station
                start_order = item_group[item_group['station_id'] == start_station].iloc[0]
                dest_order = item_group[item_group['station_id'] == dest_station].iloc[0]
                
                # Calculate price difference percentage
                price_diff_pct = (dest_order['price'] - start_order['price']) / start_order['price']
                
                # Check if profit margin meets threshold
                if price_diff_pct >= PROFIT_MARGIN_THRESHOLD:
                    pair = {
                        'type_id': int(type_id),
                        'start_station_id': int(start_station),
                        'dest_station_id': int(dest_station),
                        'price_start': float(start_order['price']),
                        'price_dest': float(dest_order['price']),
                        'volume_remain_start': int(start_order['volume_remain']),
                        'volume_remain_dest': int(dest_order['volume_remain']),
                        'supply_start': int(start_order['supply']),
                        'supply_dest': int(dest_order['supply'])
                    }
                    
                    trade_pairs.append(pair)
        
        # Convert to DataFrame
        result_df = pd.DataFrame(trade_pairs)
        
        # Add historical data if available
        if prices_df is not None and volumes_df is not None:
            logger.info("Adding historical data to trade pairs")
            result_df = add_historical_data(result_df, prices_df, volumes_df)
        
        logger.info(f"Generated {len(result_df)} trade pairs with profit potential")
        return result_df
    
    except Exception as e:
        logger.error(f"Error generating trade pairs: {e}")
        return None

def add_historical_data(trade_pairs_df, prices_df, volumes_df):
    """Add historical price and volume data to trade pairs."""
    try:
        # Make sure type_id and station_id are the same data type for merging
        prices_df['type_id'] = prices_df['type_id'].astype(np.int32)
        prices_df['location_id'] = prices_df['location_id'].astype(np.int32)
        volumes_df['type_id'] = volumes_df['type_id'].astype(np.int32)
        volumes_df['location_id'] = volumes_df['location_id'].astype(np.int32)
        
        # Select only the most recent data for each type/location
        prices_df = prices_df.sort_values('date', ascending=False)
        prices_df = prices_df.drop_duplicates(subset=['type_id', 'location_id'], keep='first')
        
        volumes_df = volumes_df.sort_values('date', ascending=False)
        volumes_df = volumes_df.drop_duplicates(subset=['type_id', 'location_id'], keep='first')
        
        # Select only needed columns from historical data
        needed_price_cols = ['sell_price_low', 'sell_price_avg']
        needed_volume_cols = ['sell_volume_avg']
        
        # Join historical price data for destination station
        result_df = trade_pairs_df.merge(
            prices_df[['type_id', 'location_id'] + needed_price_cols],
            left_on=['type_id', 'dest_station_id'],
            right_on=['type_id', 'location_id'],
            how='left'
        )
        
        # Rename columns to indicate destination
        for col in needed_price_cols:
            result_df = result_df.rename(columns={col: f"{col}_dest"})
        
        # Remove redundant location_id column
        result_df = result_df.drop('location_id', axis=1)
        
        # Join historical volume data for destination station
        result_df = result_df.merge(
            volumes_df[['type_id', 'location_id'] + needed_volume_cols],
            left_on=['type_id', 'dest_station_id'],
            right_on=['type_id', 'location_id'],
            how='left'
        )
        
        # Rename columns to indicate destination
        for col in needed_volume_cols:
            result_df = result_df.rename(columns={col: f"{col}_dest"})
        
        # Remove redundant location_id column
        result_df = result_df.drop('location_id', axis=1)
        
        return result_df
        
    except Exception as e:
        logger.error(f"Error adding historical data: {e}")
        # Return original DataFrame if there's an error
        return trade_pairs_df

def main():
    """Main function to generate trade pair analysis."""
    try:
        ensure_directories()
        
        # Load current orders
        orders_df = load_current_orders()
        if orders_df is None:
            logger.error("Cannot proceed without current orders data")
            sys.exit(1)
        
        # Load historical data
        prices_df, volumes_df = load_historic_data()
        
        # Generate trade pairs
        trade_pairs = generate_trade_pairs(orders_df, prices_df, volumes_df)
        if trade_pairs is None or len(trade_pairs) == 0:
            logger.warning("No profitable trade pairs found")
            # Create empty file to avoid errors
            empty_df = pd.DataFrame(columns=[
                'type_id', 'start_station_id', 'dest_station_id', 
                'price_start', 'price_dest'
            ])
            empty_df.to_csv(os.path.join(PROCESSED_DIR, "trade_analysis.csv"), index=False)
            return
        
        # Save trade pairs analysis
        output_path = os.path.join(PROCESSED_DIR, "trade_analysis.csv")
        trade_pairs.to_csv(output_path, index=False)
        logger.info(f"Saved trade analysis to {output_path}")
        
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
