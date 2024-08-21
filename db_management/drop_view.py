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
drop_view_sql = """
DROP VIEW price_difference;
"""
with engine.connect() as conn:
    conn.execute(text(drop_view_sql))
    print("View dropped successfully.")