from pathlib import Path
import argparse
import os
import subprocess
from typing import Optional

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
def run_python_file(
    py_file: Path,
    load_type: str,
    process_month: Optional[str] = None,
) -> None:
    logger = get_run_logger()
    logger.info(
        f"Running Python file: {py_file} | load_type={load_type} | process_month={process_month}"
    )

    cmd = [
        "python",
        str(py_file),
        "--load-type",
        load_type,
    ]

    if process_month:
        cmd.extend(["--process-month", process_month])

    subprocess.run(
        cmd,
        check=True,
        env=env,
    )


@task(name="run-spark-file", retries=2, retry_delay_seconds=5)
def run_spark_file(
    py_file: Path,
    load_type: str,
    process_month: Optional[str] = None,
) -> None:
    logger = get_run_logger()
    logger.info(
        f"Running Spark file: {py_file} | load_type={load_type} | process_month={process_month}"
    )

    cmd = [
        "spark-submit",
        str(py_file),
        "--load-type",
        load_type,
    ]

    if process_month:
        cmd.extend(["--process-month", process_month])

    subprocess.run(
        cmd,
        check=True,
        env=env,
    )


@flow(name="taxi-data-pipeline")
def taxi_pipeline(
    load_type: str = "full",
    process_month: Optional[str] = None,
) -> None:
    logger = get_run_logger()
    logger.info(
        f"Starting taxi data pipeline | load_type={load_type} | process_month={process_month}"
    )

    if load_type not in {"full", "incremental"}:
        raise ValueError("load_type must be 'full' or 'incremental'")

    if load_type == "incremental" and not process_month:
        raise ValueError(
            "For incremental load you must provide --process-month in format YYYY-MM"
        )

    # 1. Create schemas and raw tables
    run_sql_file(BASE_DIR / "sql" / "raw" / "create_schemas_and_raw_tables.sql")

    # 2. Load source files into RAW
    run_python_file(
        BASE_DIR / "scripts" / "load_to_raw_taxi_zone_lookup.py",
        load_type=load_type,
        process_month=process_month,
    )
    run_python_file(
        BASE_DIR / "scripts" / "load_to_raw_yellow_taxi_trips_2023.py",
        load_type=load_type,
        process_month=process_month,
    )

    # 3. Run scalable Spark processing
    run_spark_file(
        BASE_DIR / "spark_jobs" / "yellow_taxi_spark_job.py",
        load_type=load_type,
        process_month=process_month,
    )

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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run taxi data pipeline with Prefect.")
    parser.add_argument(
        "--load-type",
        choices=["full", "incremental"],
        default="full",
    )
    parser.add_argument(
        "--process-month",
        required=False,
        help="Month to process in format YYYY-MM, e.g. 2023-12",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    taxi_pipeline(
        load_type=args.load_type,
        process_month=args.process_month,
    )
