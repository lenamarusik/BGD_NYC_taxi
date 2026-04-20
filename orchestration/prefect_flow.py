from pathlib import Path
import os
import subprocess

from dotenv import load_dotenv
from prefect import flow, task, get_run_logger

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent

DB_HOST = os.environ["DB_HOST"]
DB_PORT = os.environ["DB_PORT"]
DB_NAME = os.environ["DB_NAME"]
DB_USER = os.environ["DB_USER"]
DB_PASSWORD = os.environ["DB_PASSWORD"]

env = os.environ.copy()
env["PGPASSWORD"] = DB_PASSWORD


@task(name="run-sql-file", retries=2, retry_delay_seconds=5)
def run_sql_file(sql_file: Path) -> None:
    logger = get_run_logger()
    logger.info(f"Running SQL file: {sql_file}")

    subprocess.run(
        [
            "psql",
            "-h",
            DB_HOST,
            "-p",
            DB_PORT,
            "-U",
            DB_USER,
            "-d",
            DB_NAME,
            "-f",
            str(sql_file),
        ],
        check=True,
        env=env,
    )


@task(name="run-python-file", retries=2, retry_delay_seconds=5)
def run_python_file(py_file: Path) -> None:
    logger = get_run_logger()
    logger.info(f"Running Python file: {py_file}")

    subprocess.run(
        [
            "python",
            str(py_file),
        ],
        check=True,
        env=env,
    )


@task(name="run-spark-file", retries=2, retry_delay_seconds=5)
def run_spark_file(py_file: Path) -> None:
    logger = get_run_logger()
    logger.info(f"Running Spark file: {py_file}")

    subprocess.run(
        [
            "spark-submit",
            str(py_file),
        ],
        check=True,
        env=env,
    )


@flow(name="taxi-data-pipeline")
def taxi_pipeline() -> None:
    logger = get_run_logger()
    logger.info("Starting taxi data pipeline")

    # 1. Create schemas and raw tables
    run_sql_file(BASE_DIR / "sql" / "raw" / "create_schemas_and_raw_tables.sql")

    # 2. Load source files into RAW
    run_python_file(BASE_DIR / "scripts" / "load_to_raw_taxi_zone_lookup.py")
    run_python_file(BASE_DIR / "scripts" / "load_to_raw_yellow_taxi_trips_2023.py")

    # 3. Run scalable Spark processing
    run_spark_file(BASE_DIR / "spark_jobs" / "yellow_taxi_spark_job.py")

    # 4. Transform RAW -> SILVER
    run_sql_file(BASE_DIR / "sql" / "silver" / "raw_to_silver_taxi_zone_lookup.sql")
    run_sql_file(BASE_DIR / "sql" / "silver" / "raw_to_silver_yellow_taxi_trips_2023.sql")

    # 5. Transform SILVER -> GOLD
    run_sql_file(BASE_DIR / "sql" / "gold" / "silver_to_gold_daily_revenue.sql")
    run_sql_file(BASE_DIR / "sql" / "gold" / "silver_to_gold_monthly_summary.sql")
    run_sql_file(BASE_DIR / "sql" / "gold" / "silver_to_gold_payment_type_summary.sql")
    run_sql_file(BASE_DIR / "sql" / "gold" / "silver_to_gold_taxi_zone_usage.sql")

    # 6. Add constraints
    run_sql_file(BASE_DIR / "sql" / "gold" / "adding_pk_setting_nn.sql")

    logger.info("Pipeline finished successfully.")


if __name__ == "__main__":
    taxi_pipeline()
