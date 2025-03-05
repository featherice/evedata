import pandas as pd

# Trade hub station IDs
hubs = [60003760, 60008494, 60004588, 60005686, 60011866]

# Read CSV (decompressed)
df = pd.read_csv("market-orders-latest.v3.csv")

# Filter sell orders for hubs
sell_orders = df[
    (df["is_buy_order"] == False) & 
    (df["station_id"].isin(hubs))
][["price", "type_id", "volume_remain", "volume_total", "station_id"]]

# Save to new CSV
sell_orders.to_csv("sell_orders_filtered.csv", index=False)
