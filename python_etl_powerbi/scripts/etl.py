import pandas as pd
from pathlib import Path

RAW = Path('data/raw/sales_data.csv')
PROCESSED = Path('data/processed/sales_cleaned.csv')

def run_etl():
    df = pd.read_csv(RAW)
    # basic cleaning
    df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')
    # drop rows with missing critical values
    df = df.dropna(subset=['order_date', 'customer', 'product'])
    # ensure numeric types
    df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce').fillna(0).astype(int)
    df['unit_price'] = pd.to_numeric(df['unit_price'], errors='coerce').fillna(0.0)
    # derived columns
    df['total_amount'] = df['quantity'] * df['unit_price']
    df['month'] = df['order_date'].dt.to_period('M').astype(str)
    # reorder columns
    cols = ['order_id','order_date','month','customer','region','product','quantity','unit_price','total_amount']
    df = df[cols]
    # save processed
    PROCESSED.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(PROCESSED, index=False)
    print(f'Processed data saved to: {PROCESSED}')
    
if __name__ == '__main__':
    run_etl()
