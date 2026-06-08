# Airflow-MongoDB Data Pipeline Project

## Project Overview

This project demonstrates the implementation of an end-to-end data engineering pipeline using Apache Airflow, Python, Pandas, Docker, and MongoDB.

The pipeline is designed to:

1. Detect the arrival of a data file using an Airflow FileSensor.
2. Validate whether the file contains data.
3. Process and clean the data through multiple transformation tasks.
4. Generate a processed dataset.
5. Trigger a second DAG using Airflow Dataset-aware Scheduling.
6. Load the processed data into MongoDB.
7. Execute analytical queries on the loaded data.

---

## Technologies Used

* Apache Airflow 2.10.0
* Python 3.12
* Pandas
* MongoDB
* Docker
* Docker Compose
* PyMongo

---

## Project Architecture

```text
Incoming CSV File
        │
        ▼
 FileSensor
        │
        ▼
 BranchPythonOperator
   ┌───────────────┐
   │               │
   ▼               ▼
Empty File     Process Data
 Log Task          │
                   ▼
            TaskGroup
                   │
        ┌──────────┼──────────┐
        │          │          │
        ▼          ▼          ▼
 Replace Nulls   Sort Data   Clean Content
                   │
                   ▼
         Processed Dataset
                   │
                   ▼
      Dataset-Aware Scheduling
                   │
                   ▼
         load_to_mongodb_dag
                   │
                   ▼
              MongoDB
```

---

## Dataset

Dataset Used:

```text
tiktok_google_play_reviews.csv
```

The dataset contains TikTok application reviews collected from the Google Play Store.

---

## Project Structure

```text
airflow-mongodb-project/
│
├── dags/
│   ├── process_reviews_dag.py
│   └── load_to_mongodb_dag.py
│
├── incoming/
│   └── tiktok_google_play_reviews.csv
│
├── processed/
│   └── processed_tiktok_reviews.csv
│
├── logs/
│
├── query_results/
│   ├── query1_top5_comments.txt
│   ├── query2_content_less_than_5_chars.txt
│   └── query3_average_rating_per_day.txt
│
├── requirements.txt
├── docker-compose.yaml
└── README.md
```

---

## DAG 1: Data Processing Pipeline

### Workflow

#### 1. FileSensor

Monitors the incoming directory and waits for the arrival of the source CSV file.

#### 2. BranchPythonOperator

Checks whether the detected file contains data.

### Branch Logic

* If file is empty:

  * Execute logging task.
* If file contains data:

  * Execute transformation TaskGroup.

---

## TaskGroup Transformations

### Replace Null Values

All null values are replaced with "-".

### Sort Data

Data is sorted using the review timestamp column.

### Clean Content

Special characters, emojis, and unwanted symbols are removed while preserving text and punctuation.

### Output

```text
processed_tiktok_reviews.csv
```

---

## DAG 2: MongoDB Loading Pipeline

This DAG is triggered automatically using Airflow Dataset-aware Scheduling whenever the processed dataset is updated.

### Workflow

1. Read processed CSV.
2. Connect to MongoDB.
3. Create database:

```text
tiktok_reviews_db
```

4. Create collection:

```text
reviews
```

5. Load all processed records into MongoDB.

---

## MongoDB Queries

### Query 1: Top 5 Frequently Occurring Comments

Objective:

Identify the most common review comments in the dataset.

Result:

```text
Good      11535
NaN       10377
Nice       9339
Nice app   4750
Good app   3554
```

---

### Query 2: Entries Where Content Length Is Less Than 5 Characters

Objective:

Identify very short review comments.

Note:

The query uses `$toString()` to handle records containing non-string values.

---

### Query 3: Average Rating Per Day

Objective:

Calculate the average review rating for each day.

Note:

The query uses `$toDate()` because the timestamp field is stored as a string in MongoDB.

---

## Challenges Encountered

### MongoDB Provider Compatibility

The MongoDB Airflow provider caused compatibility issues with the Airflow environment.

Solution:

The project was implemented using PyMongo directly instead of MongoHook.

### Data Quality Issues

Some review content values were loaded as NaN instead of text.

Solution:

Aggregation queries were updated to safely convert values using `$toString()`.

### Date Conversion Issues

MongoDB aggregation required explicit conversion of string timestamps to date objects using `$toDate()`.

---

## Key Airflow Concepts Demonstrated

* FileSensor
* BranchPythonOperator
* TaskGroup
* Dataset-aware Scheduling
* DAG Dependencies
* Data Transformation Pipelines
* MongoDB Integration
* Dockerized Deployment

---

## How to Run

### Start Containers

```bash
docker compose up -d
```

### Open Airflow

```text
http://localhost:8080
```

### Trigger Processing DAG

```text
process_reviews_dag
```

### Verify MongoDB Load

```bash
docker exec -it mongodb mongosh
```

```javascript
use tiktok_reviews_db
db.reviews.countDocuments()
```

---

## Conclusion

This project successfully implements a complete data engineering workflow using Apache Airflow and MongoDB. The solution demonstrates file-based ingestion, data validation, transformation, dataset-driven orchestration, and analytical querying of processed data. The implementation follows modular pipeline design principles and showcases practical Airflow features commonly used in production-grade data engineering environments.
