import os
import pandas as pd
from pathlib import Path
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket


@task(log_prints=True, tags=["fetch"], retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """
    Read taxi data from web into pandas DataFrame.
    """
    # Read data in chunks and concat after reading to reduce memory usage and avoid timeout.
    chunk_size = 10000
    chunks = []

    # Use a for loop to read the data in chunks.
    for chunk in pd.read_csv(dataset_url, chunksize=chunk_size):

    # Append each chunk to the list of chunks.
        chunks.append(chunk)

    # Concatenate the chunks into a single dataframe.
    df = pd.concat(chunks)

    # Print sample.
    print(df.head())
    return df


@task(log_prints=True, tags=["clean"])
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fix dtype issues.
    """
    df["lpep_pickup_datetime"] = pd.to_datetime(df["lpep_pickup_datetime"])
    df["lpep_dropoff_datetime"] = pd.to_datetime(df["lpep_dropoff_datetime"])
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df


@task(log_prints=True, tags=["write"])
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """
    Write DataFrame out locally as parquet file.
    """
    base_path = "/home/j_unspeakable/Programming/Data_Engineering_Zoomcamp/week_1/"
    path = Path(f"data/{color}/{dataset_file}.parquet")

    local_path = os.path.join(base_path, path)

    df.to_parquet(local_path, compression="gzip")
    return local_path, path


@task(log_prints=True, tags=["gcs"])
def write_gcs(local_path: Path, path: Path) -> None:
    """
    Upload local parquet file to GCS.
    """
    gcs_block = GcsBucket.load("de-zoomcamp1")
    gcs_block.upload_from_path(from_path=local_path, to_path=path)
    return


@flow()
def etl_web_to_gcs(year: int, month: int, color: str) -> None:
    """
    The main ETL function.
    """
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df)
    local_path, path = write_local(df_clean, color, dataset_file)
    write_gcs(local_path, path)


@flow()
def etl_parent_flow(year: int, months: list[int], color: str):
    """
    Parent function to invoke the main ETL function.
    """
    for month in months:
        etl_web_to_gcs(year, month, color)


if __name__ == "__main__":
    color = "green"
    months = [11]
    year = 2020
    etl_parent_flow(year, months, color)