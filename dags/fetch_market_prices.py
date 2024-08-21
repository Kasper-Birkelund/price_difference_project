
import os
import requests
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database connection setup
db_url = f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
engine = create_engine(db_url)

# API key stored in an environment variable for security
api_key = os.getenv('CRYPTOCOMPARE_API_KEY')

def fetch_market_prices():

    # get timestamps from trades
    query = """
    SELECT block_time_unix, tx_hash FROM dune_data
    WHERE tx_hash not in (SELECT DISTINCT trade_tx from market_prices);
    """

    trades = pd.read_sql(query, engine)
    
    # remove duplicates
    trades = trades.drop_duplicates()

    # Example trade timestamps to fetch historical prices for
    # These would be dynamically sourced from your trades data
    timestamps = dict(zip(trades['block_time_unix'], trades['tx_hash']))

    all_prices = pd.DataFrame()

    for ts, tx in timestamps.items():
        # Fetch prices 10 minutes before and after the timestamp to ensure coverage
        url = f"https://min-api.cryptocompare.com/data/v2/histoday?fsym=ETH&tsym=USD&toTs={ts}&limit=1&api_key={api_key}"
        response = requests.get(url)
        data = response.json()
        
        # Convert to DataFrame
        df = pd.DataFrame(data['Data']['Data'])
        df = df.tail(1)
        df['time'] = pd.to_datetime(df['time'], unit='s')
        df['trade_unix_time'] = ts
        df['trade_tx'] = tx
        all_prices = pd.concat([all_prices, df], ignore_index=True)

    # Save to PostgreSQL
    all_prices.to_sql('market_prices', engine, if_exists='append', index=False)
    
if __name__ == "__main__":
    fetch_market_prices()