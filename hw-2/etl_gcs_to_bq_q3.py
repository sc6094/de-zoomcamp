from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import  GcpCredentials
from pathlib import Path
import pandas as pd


@task(retries=3)
def extract_from_gcp(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path= f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("prefect-de-zoomcamp-jjay")
    gcs_block.get_directory(from_path = gcs_path, local_path="../../data")
    return Path(f"../../data/{gcs_path}")



@task()
def write_bq(df: pd.DataFrame) -> None:
    """write datarame to bigquery"""
    gcp_credentials_block = GcpCredentials.load("de-zoomcamp-prefect-gcp-creds")
    df.to_gbq(
        destination_table="dezoomcamp.rides",
        project_id="data-camp-375803",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize="500_000",
        if_exists="append",
    )

@flow()
def etl_gcs_to_bq(month: int, year: int, color: str):
    """Main ETL flow to load data into Big Query"""
    path = extract_from_gcp(color, year, month)
    df = pd.read_parquet(path)
    write_bq(df)
    return len(df)

@flow()
def etl_parent_flow(months: list[int], year: int, color: str):
    count = 0
    for month in months:
        rows = etl_gcs_to_bq(month, year, color)
        count += rows
    print("Rows processed: ", count)

if __name__ == "__main__":
    color = "yellow"
    months = [2, 3]
    year = 2019
    etl_parent_flow(months, year, color)



