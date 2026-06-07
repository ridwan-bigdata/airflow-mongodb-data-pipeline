from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.task_group import TaskGroup

from datetime import datetime

import pandas as pd
import re


# FILE PATHS

INPUT_FILE = "/opt/airflow/incoming/tiktok_google_play_reviews.csv"

OUTPUT_FILE = "/opt/airflow/processed/processed_tiktok_reviews.csv"

processed_dataset = Dataset(OUTPUT_FILE)


# BRANCH FUNCTION

def check_file():

    df = pd.read_csv(INPUT_FILE)

    if df.empty:
        return "empty_file"

    return "transformations.replace_nulls"


# TASK 1
# Replace null values with "-"


def replace_nulls():

    df = pd.read_csv(INPUT_FILE)

    df.fillna("-", inplace=True)

    df.to_csv(
        OUTPUT_FILE,
        index=False
    )


# TASK 2
# Sort by date column (at)


def sort_by_date():

    df = pd.read_csv(OUTPUT_FILE)

    df["at"] = pd.to_datetime(df["at"])

    df = df.sort_values("at")

    df.to_csv(
        OUTPUT_FILE,
        index=False
    )


# TASK 3
# Remove emojis and special characters


def clean_content():

    df = pd.read_csv(OUTPUT_FILE)

    def clean_text(text):

        return re.sub(
            r"[^A-Za-z0-9\s.,!?;:'\"()-]",
            "",
            str(text)
        )

    df["content"] = df["content"].apply(clean_text)

    df.to_csv(
        OUTPUT_FILE,
        index=False
    )

# DAG

with DAG(
    dag_id="process_reviews_dag",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    schedule=None,
    tags=["assessment"]
) as dag:

    # SENSOR

    wait_for_file = FileSensor(
        task_id="wait_for_file",
        filepath=INPUT_FILE,
        poke_interval=10,
        timeout=300
    )

    # BRANCH

    check_input_file = BranchPythonOperator(
        task_id="check_file",
        python_callable=check_file
    )

    # EMPTY FILE TASK

    empty_file = BashOperator(
        task_id="empty_file",
        bash_command='echo "INPUT FILE IS EMPTY"'
    )

    # TASK GROUP

    with TaskGroup(
        group_id="transformations"
    ) as transformations:

        replace_task = PythonOperator(
            task_id="replace_nulls",
            python_callable=replace_nulls
        )

        sort_task = PythonOperator(
            task_id="sort_by_date",
            python_callable=sort_by_date
        )

        clean_task = PythonOperator(
            task_id="clean_content",
            python_callable=clean_content,
            outlets=[processed_dataset]
        )

        replace_task >> sort_task >> clean_task

    # DEPENDENCIES

    wait_for_file >> check_input_file

    check_input_file >> empty_file

    check_input_file >> transformations