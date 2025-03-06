#!/usr/bin/env python3
"""
Generate trade pairs from EVE Online market data.
Creates profitable trade pairs between hubs.
"""
import pandas as pd
import numpy as np
import logging
from pathlib import Path
import io

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
INPUT_DIR = Path("data/processed")
OUTPUT_DIR = Path("data/results")
PROFIT_THRESHOLD = 0.1  # 10% minimum profit margin

def ensure_dirs():
    """Ensure all required directories exist."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def load_current_data():
    """Load current market data."""
    try:
        file_path = INPUT_DIR / "current_market_data.csv"
        if not file_path.exists():
            logger.error(f"Current market data file not found: {file_path}")
            return None
            
        df = pd.read_csv(file_path)
        logger.info(f"Loaded current market data with {len(df)} rows")
        return df
    except Exception as e:
        logger.error(f"Error loading current market data: {e}")
        raise

def load_historic_data():
    """Load historic market data if available."""
    try:
        file_path = INPUT_DIR / "historic_market_data.csv"
        if not file_path.exists():
            logger.warning(f"Historic market data file not found: {file_path}")
            return None
            
        df = pd.read_csv(file_path)
        logger.info(f"Loaded historic market data with {len(df)} rows")
        return df
    except Exception as e:
        logger.error(f"Error loading historic data: {e}")
        return None

def generate_trade_pairs(current_df, historic_df=None):
    """Generate trade pairs with profitability analysis."""
    try:
        pairs = []
        
        # Get unique type_ids
        type_ids = current_df['type_id'].unique()
        logger.info(f"Processing {len(type_ids)} unique item types")
        
        for type_id in type_ids:
            # Get data for this type_id
            type_data = current_df[current_df['type_id'] == type_id]
            
            # Generate all possible hub pairs for this type
            stations = type_data['station_id'].unique()
            
            if len(stations) < 2:
                continue
                
            for start_station in stations:
                start_data = type_data[type_data['station_id'] == start_station].iloc[0]
                
                for dest_station in stations:
                    if start_station == dest_station:
                        continue
                        
                    dest_data = type_data[type_data['station_id'] == dest_station].iloc[0]
                    
                    # Calculate profit margin
                    price_diff = dest_data['price'] - start_data['price']
                    profit_margin = price_diff / start_data['price']
                    
                    # Only include if profit margin exceeds threshold
                    if profit_margin >= PROFIT_THRESHOLD:
                        pair_data = {
                            'type_id': type_id,
                            'start_station_id': start_station,
                            'dest_station_id': dest_station,
                            'price_start': start_data['price'],
                            'price_dest': dest_data['price'],
                            'volume_remain_start': start_data['volume_remain'],
                            'volume_remain_dest': dest_data['volume_remain'],
                            'supply_start': start_data['supply'],
                            'supply_dest': dest_data['supply'],
                            'profit_margin': profit_margin
                        }
                        
                        # Add historic data if available
                        if historic_df is not None:
                            hist_dest = historic_df[(historic_df['type_id'] == type_id) & 
                                                   (historic_df['station_id'] == dest_station)]
                            
                            if not hist_dest.empty:
                                row = hist_dest.iloc[0]
                                pair_data.update({
                                    'sell_price_low_dest': row['sell_price_low'],
                                    'sell_price_avg_dest': row['sell_price_avg'],
                                    'sell_volume_avg_dest': row['sell_volume_avg']
                                })
                            else:
                                # Set defaults if no historic data
                                pair_data.update({
                                    'sell_price_low_dest': None,
                                    'sell_price_avg_dest': None,
                                    'sell_volume_avg_dest': None
                                })
                        
                        pairs.append(pair_data)
        
        # Create dataframe from pairs
        pairs_df = pd.DataFrame(pairs)
        
        if pairs_df.empty:
            logger.warning("No profitable trade pairs found")
            return pairs_df
            
        # Sort by profit margin
        pairs_df = pairs_df.sort_values('profit_margin', ascending=False)
        logger.info(f"Generated {len(pairs_df)} profitable trade pairs")
        
        return pairs_df
    except Exception as e:
        logger.error(f"Error generating trade pairs: {e}")
        raise

def main():
    """Main function to generate trade pairs."""
    try:
        ensure_dirs()
        
        # Load data
        current_df = load_current_data()
        if current_df is None:
            return
            
        historic_df = load_historic_data()
        
        # Generate trade pairs
        pairs_df = generate_trade_pairs(current_df, historic_df)
        
        if not pairs_df.empty:
            # Save results
            output_path = OUTPUT_DIR / "trade_pairs.csv"
            pairs_df.to_csv(output_path, index=False)
            logger.info(f"Saved {len(pairs_df)} trade pairs to {output_path}")
    except Exception as e:
        logger.error(f"Error in main processing: {e}")
        raise

if __name__ == "__main__":
    main()
