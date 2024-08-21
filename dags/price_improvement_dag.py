import os
import sys

# Append the directory containing 'src' to PYTHONPATH
# sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from fetch_dune_data import fetch_dune_data
from fetch_market_prices import fetch_market_prices
from calculate_price_improvement import calculate_price_improvement

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 28),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'price_improvement_dag',
    default_args=default_args,
    description='DAG to calculate price improvement',
    schedule_interval='@daily'
)

fetch_dune_data_task = PythonOperator(
    task_id='fetch_dune_data',
    python_callable=fetch_dune_data,
    dag=dag
)

fetch_market_prices_task = PythonOperator(
    task_id='fetch_market_prices',
    python_callable=fetch_market_prices,
    dag=dag
)

calculate_price_improvement_task = PythonOperator(
    task_id='calculate_price_improvement',
    python_callable=calculate_price_improvement,
    dag=dag
)

fetch_dune_data_task >> fetch_market_prices_task >> calculate_price_improvement_task
