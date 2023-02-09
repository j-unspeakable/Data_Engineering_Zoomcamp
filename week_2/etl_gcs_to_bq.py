import pandas as pd
from pathlib import Path
from datetime import timedelta
from prefect import flow, task
from prefect_gcp import GcpCredentials
from prefect.tasks import task_input_hash
from prefect_gcp.cloud_storage import GcsBucket


@task(log_prints=True, tags=["extract"], cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """
    Download trip data from GCS.
    """
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
    return Path(f"../data/{gcs_path}")


@task(log_prints=True, tags=["read"])
def read(path: Path) -> pd.DataFrame:
    """
    Get data sample.
    """
    df = pd.read_parquet(path)
    print(df.head())
    return df


@task(log_prints=True, tags=["bq"])
def write_bq(df: pd.DataFrame) -> None:
    """
    Write DataFrame to BiqQuery
    """
    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")

    df.to_gbq(
        destination_table="dezoomcamp.rides",
        project_id="prefect-sbx-community-eng",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500000,
        if_exists="append",
    )


@flow()
def etl_gcs_to_bq(year: int, month: int, color: str):
    """
    Main ETL flow to load data into Big Query
    """
    path = extract_from_gcs(color, year, month)
    df = read(path)
    write_bq(df)


@flow()
def etl_parent_flow(year: int, months: list[int], color: str):
    """
    Parent function to invoke the main ETL function.
    """
    for month in months:
        etl_gcs_to_bq(year, month, color)


if __name__ == "__main__":
    color = "yellow"
    months = [2, 3]
    year = 2019
    etl_parent_flow(months, year, color)