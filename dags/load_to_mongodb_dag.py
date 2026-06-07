from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.python import PythonOperator

from datetime import datetime
import pandas as pd
from pymongo import MongoClient

processed_dataset = Dataset(
    "/opt/airflow/processed/processed_tiktok_reviews.csv"
)


def load_to_mongodb():

    df = pd.read_csv(
        "/opt/airflow/processed/processed_tiktok_reviews.csv"
    )

    client = MongoClient(
        "mongodb://mongodb:27017/"
    )

    db = client["tiktok_reviews_db"]

    collection = db["reviews"]

    collection.delete_many({})

    collection.insert_many(
        df.to_dict("records")
    )

    client.close()


with DAG(
    dag_id="load_to_mongodb_dag",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    schedule=[processed_dataset],
    tags=["mongodb"],
) as dag:

    load_task = PythonOperator(
        task_id="load_to_mongodb",
        python_callable=load_to_mongodb
    )