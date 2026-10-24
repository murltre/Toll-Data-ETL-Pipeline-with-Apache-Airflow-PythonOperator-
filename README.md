# Toll Data ETL Pipeline with Apache Airflow (PythonOperator)

## Project overview

This project implements the optional IBM Skills Network toll-data ETL lab with Apache Airflow and `PythonOperator`. It downloads and extracts a dataset, selects fields from CSV, TSV, and fixed-width text files, combines the selected fields, uppercases vehicle types, and saves the final CSV to a staging directory.

The DAG uses Python functions for the data processing and Airflow for scheduling, dependencies, retries, and monitoring.

## Tech stack

- Apache Airflow 2.10.5: task orchestration and monitoring
- Python 3.12: ETL functions and DAG definition
- `PythonOperator`: calls Python functions as Airflow tasks
- `requests`: downloads the dataset
- `tarfile`: extracts the `.tgz` archive
- `csv`: reads, writes, and consolidates CSV data
- Ubuntu on WSL and VS Code: local development environment

## Workflow

```text
download_dataset
       |
  untar_dataset
       |
       +--------------------+----------------------+
       |                    |                      |
extract_data_from_csv  extract_data_from_tsv  extract_data_from_fixed_width
       |                    |                      |
       +--------------------+----------------------+
                            |
                    consolidate_data
                            |
                      transform_data
```

1. **`download_dataset`** downloads `tolldata.tgz` into `staging/`.
2. **`untar_dataset`** extracts the archive.
3. **`extract_data_from_csv`** selects the first four fields from `vehicle-data.csv`.
4. **`extract_data_from_tsv`** selects the first three fields from `tollplaza-data.tsv`, following the course solution.
5. **`extract_data_from_fixed_width`** reads characters 1-6 and 7-12 from `payment-data.txt`, following the course solution.
6. **`consolidate_data`** combines corresponding rows into a nine-field `extracted_data.csv`.
7. **`transform_data`** converts `Vehicle type` to uppercase and writes `transformed_data.csv`.

**Parallelism:** Airflow runs the three extraction tasks independently after `untar_dataset` succeeds. `consolidate_data` waits for all three to succeed. The consolidation pairs rows by position with Python's `zip`; it does not join on a key, and extra rows beyond the shortest input are not included.

## Project layout in the local environment

```text
~/IBMDEexercises/AirflowKafka/
├── .venv/                                  # Existing Python virtual environment
└── airflow_home/
    └── dags/
        ├── ETL_toll_data.py                # Earlier BashOperator project
        └── python_etl/
            ├── ETL_toll_data_python.py      # This DAG
            └── staging/
                ├── tolldata.tgz             # Downloaded archive
                ├── vehicle-data.csv          # Extracted source
                ├── tollplaza-data.tsv        # Extracted source
                ├── payment-data.txt          # Extracted source
                ├── csv_data.csv              # Intermediate output
                ├── tsv_data.csv              # Intermediate output
                ├── fixed_width_data.csv      # Intermediate output
                ├── extracted_data.csv        # Nine-column consolidated file
                └── transformed_data.csv      # Final output
```

The archive and generated data files are not included in this bundle; the reproduction guide downloads them again.

## Key concepts

- **DAG:** the directed acyclic graph describes tasks and dependencies.
- **PythonOperator:** links an Airflow task to a Python function with `python_callable`.
- **Task dependencies (`>>`):** control when downstream tasks may execute.
- **Parallel extraction:** the three source files are processed independently after the archive is extracted.
- **`AIRFLOW_HOME`:** selects the Airflow home used by the terminal and Airflow process.
- **Staging directory:** holds the archive, extracted source files, intermediate CSVs, and final output.
- **Retries:** a failed task can be attempted again once, after a five-minute delay.
- **Grid view and logs:** show task status and help diagnose errors.

## Reproduce the project

Follow [`docs/REPRODUCE_AND_COMMANDS.md`](docs/REPRODUCE_AND_COMMANDS.md) for commands with comments, setup, checks, and troubleshooting.

## Output

Final file:

`$AIRFLOW_HOME/dags/python_etl/staging/transformed_data.csv`

It should contain these nine columns in order: `Rowid`, `Timestamp`, `Anonymized Vehicle number`, `Vehicle type`, `Number of axles`, `Tollplaza id`, `Tollplaza code`, `Type of Payment code`, and `Vehicle Code`.

## Notes before publishing

- Replace `your_email@example.com` in the DAG if you intend to use Airflow email alerts. An address in `default_args` does not configure SMTP delivery by itself.
- The DAG ID is `ETL_toll_data_python` to avoid colliding with the previous BashOperator project (`ETL_toll_data`).
- The source field selection follows the sample solution supplied with the optional PythonOperator lab. Specifically, the TSV function takes the first three fields and the fixed-width function slices `line[0:6]` and `line[6:12]`.
- Do not commit the `.venv/`, Airflow logs, dataset archive, or generated staging files.
- Capture screenshots from your own Airflow instance; see [`screenshots/SCREENSHOT_CHECKLIST.md`](screenshots/SCREENSHOT_CHECKLIST.md).

**Project focus:** Batch ETL · Apache Airflow · PythonOperator · Python file processing · Workflow orchestration
