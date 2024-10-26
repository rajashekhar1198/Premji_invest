from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests

def fetch_articles(ticker):
    # Scrape articles for the given ticker
    # Example implementation
    print(f"Fetching articles for {ticker}")

def clean_and_process_data():
    # Clean and deduplicate data
    print("Cleaning and processing data")

def generate_sentiment_scores():
    # Mock sentiment score generation
    print("Generating sentiment scores")

def persist_scores():
    # Persist the scores into the database
    print("Persisting scores")

with DAG(
    'pipeline_1',
    default_args={'owner': 'airflow', 'start_date': datetime(2024, 10, 25)},
    schedule_interval='0 19 * * 1-5',
    catchup=False
) as dag1:
    
    fetch_hdfc = PythonOperator(task_id='fetch_hdfc_articles', python_callable=fetch_articles, op_kwargs={'ticker': 'HDFC'})
    fetch_tata = PythonOperator(task_id='fetch_tata_articles', python_callable=fetch_articles, op_kwargs={'ticker': 'Tata Motors'})
    clean_data = PythonOperator(task_id='clean_and_process_data', python_callable=clean_and_process_data)
    sentiment_scores = PythonOperator(task_id='generate_sentiment_scores', python_callable=generate_sentiment_scores)
    persist_data = PythonOperator(task_id='persist_scores', python_callable=persist_scores)

    fetch_hdfc >> fetch_tata >> clean_data
