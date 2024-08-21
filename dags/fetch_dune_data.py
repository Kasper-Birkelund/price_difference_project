
import os
from dotenv import load_dotenv, find_dotenv
from dune_client.types import QueryParameter
from dune_client.client import DuneClient
from dune_client.query import QueryBase
import psycopg2
from sqlalchemy import create_engine
import pandas as pd

# get environment variables
load_dotenv(find_dotenv(), override=True)

# setup Dune Python client
dune = DuneClient.from_env()

# Database connection URL
db_url = f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
engine = create_engine(db_url)

def fetch_dune_data():
    
    # Get latest price from database
    query = "SELECT max(block_time_unix) FROM dune_data;"
    last_swap = pd.read_sql(query, engine)
    last_swap = last_swap['max'][0]

    # set the dune query id:
    dune_query_id = 4002884

    # query the query (execute and get latest result)
    query = QueryBase(
        query_id=dune_query_id,

        params=[
            QueryParameter.number_type(name="day_limit", value=last_swap), # default is 1704067200 or 2024-07-01
        ],
    )

    query_result = dune.run_query_dataframe(
    query=query
    # , ping_frequency = 10 # uncomment to change the seconds between checking execution status, default is 1 second
    # , performance="large" # uncomment to run query on large engine, default is medium
    # , batch_size = 5_000 # uncomment to change the maximum number of rows to retrieve per batch of results, default is 32_000
    ) 

    # Remove ' UTC' from the datetime strings
    query_result['block_time'] = pd.to_datetime(query_result['block_time'].str.replace(' UTC', ''))

    # Convert the datetime objects to Unix timestamps (seconds since epoch)
    query_result['block_time_unix'] = query_result['block_time'].astype(int) // 10**9
    
    # add sell_price
    query_result['sell_price'] = query_result['dst_tokens'] / query_result['src_tokens']

    # Save to PostgreSQL
    # Use SQLAlchemy to create the table in a specified schema
    query_result.to_sql('dune_data', engine, schema='public', if_exists='append', index=False)
    
if __name__ == "__main__":
    fetch_dune_data()