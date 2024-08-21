import os
from sqlalchemy import create_engine, text
import pandas as pd
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database connection setup
db_url = f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
engine = create_engine(db_url)

# SQL statement to create a view
create_view_sql = """
CREATE OR REPLACE VIEW price_difference AS
SELECT
    d.block_time,
    d.tx_hash,
    COALESCE(CAST(d.sell_price AS NUMERIC), 0) AS sell_price,
    COALESCE(CAST(m.close AS NUMERIC), 0) AS market_close,
    ((COALESCE(CAST(d.sell_price AS NUMERIC), 0) - COALESCE(CAST(m.close AS NUMERIC), 0)) / COALESCE(CAST(m.close AS NUMERIC), 0)) * 100 AS difference_percentage
FROM
    dune_data d
JOIN
    market_prices m ON m.trade_tx = d.tx_hash;
"""
with engine.connect() as conn:
    conn.execute(text(create_view_sql))
    print("View created successfully.")
