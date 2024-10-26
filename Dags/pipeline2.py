from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def mean_age_by_occupation():
    print("Calculating mean age by occupation")

def top_20_highest_rated_movies():
    print("Finding top 20 highest rated movies")

def top_genres_by_occupation_and_age():
    print("Finding top genres by occupation and age")

def top_10_similar_movies():
    print("Finding top 10 similar movies")

with DAG(
    'pipeline_2',
    default_args={'owner': 'airflow', 'start_date': datetime(2024, 10, 25)},
    schedule_interval='0 20 * * 1-5',
    catchup=False
) as dag2:

    mean_age_task = PythonOperator(task_id='mean_age_by_occupation', python_callable=mean_age_by_occupation)
    top_movies_task = PythonOperator(task_id='top_20_highest_rated_movies', python_callable=top_20_highest_rated_movies)
    top_genres_task = PythonOperator(task_id='top_genres_by_occupation_and_age', python_callable=top_genres_by_occupation_and_age)
    similar_movies_task = PythonOperator(task_id='top_10_similar_movies', python_callable=top_10_similar_movies)

    mean_age_task >> top_movies_task >> top_genres_task >> similar_movies_task
