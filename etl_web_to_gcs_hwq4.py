import pandas as pd
from pathlib import Path
from prefect import task, flow
from prefect_gcp.cloud_storage import GcsBucket
#from prefect.filesystems import GitHub
#github_block = GitHub.load("github-flow-hw")


@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """
    Read taxi data from web into pandas DataFrame
    """
    df = pd.read_csv(dataset_url)

    return df


@task(log_prints=True)
def clean(df=pd.DataFrame) -> pd.DataFrame:
    """
    Fix dtype issues.
    """
    #print(df.columns)
    df["lpep_pickup_datetime"] = pd.to_datetime(df["lpep_pickup_datetime"])
    df["lpep_dropoff_datetime"] = pd.to_datetime(df["lpep_dropoff_datetime"])

    print(f"### NUMBER OF ROWS: {len(df)}")
    return df


@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """
    Write DataFrame out locally as parquet file
    """
    #path = Path(f"data/{color}/{dataset_file}.parquet")
    path = Path(f"/home/juan/data-engineering-zoomcamp/NY_trips_project/flows/02_gcp/data/{color}/{dataset_file}.parquet")
    path_git = Path(f"data/{color}/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")

    return path, path_git


@task()
def write_gcs(path: Path, path_git: Path) -> None:
    """Upload local parquet file to GCS. Min 22:20"""

    gcs_block = GcsBucket.load("zoom-gcs") 
    gcs_block.upload_from_path(
        from_path=f"{path}",
        to_path=path_git
    )
    return


@flow(retries=3)
def etl_web_to_gcs() -> None:
    """Main ETL function"""
    color = "green"
    year = 2020
    month = 11
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df)
    path, path_git = write_local(df_clean, color, dataset_file)
    write_gcs(path, path_git)



if __name__ == '__main__':
    etl_web_to_gcs()