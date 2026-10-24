"""Airflow DAG for the Toll Data ETL project using PythonOperator.

The workflow downloads and extracts a toll-data archive, extracts fields from
CSV, TSV, and fixed-width text files, consolidates the outputs, uppercases the
vehicle type, and saves the final CSV to the staging directory.

Expected local layout:
  $AIRFLOW_HOME/dags/python_etl/ETL_toll_data_python.py
  $AIRFLOW_HOME/dags/python_etl/staging/
"""

from datetime import datetime, timedelta
import csv
import os
import tarfile
from pathlib import Path

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator


# Resolve the project directories from AIRFLOW_HOME so the paths work across
# machines. AIRFLOW_HOME must be exported in the terminal running Airflow.
AIRFLOW_HOME = Path(os.environ["AIRFLOW_HOME"])
PROJECT_DIR = AIRFLOW_HOME / "dags" / "python_etl"
STAGING_DIR = PROJECT_DIR / "staging"
ARCHIVE_PATH = STAGING_DIR / "tolldata.tgz"

# Dataset toll-data ETL.
SOURCE_URL = (
    "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/"
    "IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz"
)


# Task 1: Download the source archive into staging.
def download_dataset():
    # Create the staging directory if it does not exist yet.
    STAGING_DIR.mkdir(parents=True, exist_ok=True)

    # Stream the download in chunks rather than keeping the whole archive in RAM.
    with requests.get(SOURCE_URL, stream=True, timeout=60) as response:
        response.raise_for_status()  # Raise an error if the download failed.
        with open(ARCHIVE_PATH, "wb") as output_file:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    output_file.write(chunk)

    print(f"Dataset downloaded to {ARCHIVE_PATH}")


# Task 2: Extract the downloaded .tgz archive into staging.
def untar_dataset():
    # filter='data' restricts unsafe paths during extraction (Python 3.12+).
    with tarfile.open(ARCHIVE_PATH, "r:gz") as archive:
        archive.extractall(path=STAGING_DIR, filter="data")

    print(f"Dataset extracted to {STAGING_DIR}")


# Task 3: Extract the four vehicle fields from vehicle-data.csv.
def extract_data_from_csv():
    input_file = STAGING_DIR / "vehicle-data.csv"
    output_file = STAGING_DIR / "csv_data.csv"

    with open(input_file, "r", newline="") as infile, open(
        output_file, "w", newline=""
    ) as outfile:
        writer = csv.writer(outfile)
        # Output headers make the meaning of each selected column explicit.
        writer.writerow(
            ["Rowid", "Timestamp", "Anonymized Vehicle number", "Vehicle type"]
        )

        for line in infile:
            # The lab's source file is comma-delimited. Select fields 1 through 4.
            row = line.rstrip("\r\n").split(",")
            if len(row) >= 4:
                writer.writerow(row[:4])

    print(f"CSV fields written to {output_file}")


# Task 4: Extract three fields from the toll plaza TSV file.
def extract_data_from_tsv():
    input_file = STAGING_DIR / "tollplaza-data.tsv"
    output_file = STAGING_DIR / "tsv_data.csv"

    with open(input_file, "r", newline="") as infile, open(
        output_file, "w", newline=""
    ) as outfile:
        writer = csv.writer(outfile)
        writer.writerow(["Number of axles", "Tollplaza id", "Tollplaza code"])

        for line in infile:
            # Split on tabs, then take the first three fields as in the lab solution.
            row = line.rstrip("\r\n").split("\t")
            if len(row) >= 3:
                writer.writerow(row[:3])

    print(f"TSV fields written to {output_file}")


# Task 5: Extract two fields from the fixed-width payment file.
def extract_data_from_fixed_width():
    input_file = STAGING_DIR / "payment-data.txt"
    output_file = STAGING_DIR / "fixed_width_data.csv"

    with open(input_file, "r") as infile, open(
        output_file, "w", newline=""
    ) as outfile:
        writer = csv.writer(outfile)
        writer.writerow(["Type of Payment code", "Vehicle Code"])

        for line in infile:
            # The lab solution uses characters 1-6 and 7-12 (Python slices 0:6, 6:12).
            writer.writerow([line[0:6].strip(), line[6:12].strip()])

    print(f"Fixed-width fields written to {output_file}")


# Task 6: Combine the three extracted files into a nine-field CSV.
def consolidate_data():
    csv_file = STAGING_DIR / "csv_data.csv"
    tsv_file = STAGING_DIR / "tsv_data.csv"
    fixed_width_file = STAGING_DIR / "fixed_width_data.csv"
    output_file = STAGING_DIR / "extracted_data.csv"

    with open(csv_file, "r", newline="") as csv_in, open(
        tsv_file, "r", newline=""
    ) as tsv_in, open(fixed_width_file, "r", newline="") as fixed_in, open(
        output_file, "w", newline=""
    ) as out_file:
        csv_reader = csv.reader(csv_in)
        tsv_reader = csv.reader(tsv_in)
        fixed_reader = csv.reader(fixed_in)
        writer = csv.writer(out_file)

        # Keep the final field order specified in the lab.
        writer.writerow(
            [
                "Rowid",
                "Timestamp",
                "Anonymized Vehicle number",
                "Vehicle type",
                "Number of axles",
                "Tollplaza id",
                "Tollplaza code",
                "Type of Payment code",
                "Vehicle Code",
            ]
        )

        # Skip the header row from each intermediate CSV file.
        next(csv_reader, None)
        next(tsv_reader, None)
        next(fixed_reader, None)

        # zip combines records by row position, not by a matching ID/key.
        for csv_row, tsv_row, fixed_row in zip(
            csv_reader, tsv_reader, fixed_reader
        ):
            writer.writerow(csv_row + tsv_row + fixed_row)

    print(f"Consolidated data written to {output_file}")


# Task 7: Uppercase the Vehicle type field and save the final file.
def transform_data():
    input_file = STAGING_DIR / "extracted_data.csv"
    output_file = STAGING_DIR / "transformed_data.csv"

    with open(input_file, "r", newline="") as infile, open(
        output_file, "w", newline=""
    ) as outfile:
        reader = csv.DictReader(infile)
        writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
        writer.writeheader()

        for row in reader:
            # Normalize vehicle type, for example 'car' becomes 'CAR'.
            row["Vehicle type"] = row["Vehicle type"].upper()
            writer.writerow(row)

    print(f"Transformed dataset saved to {output_file}")


# Default settings used by each DAG task.
default_args = {
    "owner": "data_engineer",
    "start_date": datetime(2026, 10, 22),
    "email": ["your_email@example.com"],  # Replace if email alerts are configured.
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# Define the DAG and its seven PythonOperator tasks.
with DAG(
    dag_id="ETL_toll_data_python",  # Keep this ID distinct from the BashOperator DAG.
    default_args=default_args,
    description="Toll data ETL pipeline using PythonOperator",
    schedule="@daily",
    catchup=False,  # Do not create a backlog of missed scheduled runs.
    max_active_runs=1,
    tags=["ETL", "PythonOperator", "toll-data"],
) as dag:

    # Each PythonOperator calls one ordinary Python function above.
    download_task = PythonOperator(
        task_id="download_dataset",
        python_callable=download_dataset,
    )

    untar_task = PythonOperator(
        task_id="untar_dataset",
        python_callable=untar_dataset,
    )

    extract_csv_task = PythonOperator(
        task_id="extract_data_from_csv",
        python_callable=extract_data_from_csv,
    )

    extract_tsv_task = PythonOperator(
        task_id="extract_data_from_tsv",
        python_callable=extract_data_from_tsv,
    )

    extract_fixed_width_task = PythonOperator(
        task_id="extract_data_from_fixed_width",
        python_callable=extract_data_from_fixed_width,
    )

    consolidate_task = PythonOperator(
        task_id="consolidate_data",
        python_callable=consolidate_data,
    )

    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
    )

    # Download -> untar -> three independent extraction branches in parallel
    # -> consolidate after all three extractions succeed -> transform final data.
    download_task >> untar_task >> [
        extract_csv_task,
        extract_tsv_task,
        extract_fixed_width_task,
    ] >> consolidate_task >> transform_task
