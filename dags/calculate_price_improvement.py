import os
import logging
from sqlalchemy import create_engine, text
import pandas as pd
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database connection setup
db_url = f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
engine = create_engine(db_url)

def calculate_price_difference():

    # Get latest trade from database
    query = "SELECT AVG(difference_percentage) FROM price_difference;"
    avg_pi = pd.read_sql(query, engine)

    # round number
    avg_price_difference = round(avg_pi['avg'][0], 3)

    # display result
    logging.info("### The average price difference for all the trades was {} percent ###".format(avg_price_difference))
    
if __name__ == "__main__":
    calculate_price_difference()