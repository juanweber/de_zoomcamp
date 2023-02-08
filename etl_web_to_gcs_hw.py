# vid 2.2.3
import pandas as pd
from pathlib import Path
from prefect import task, flow
from prefect_gcp.cloud_storage import GcsBucket


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
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])

    print(f"Number of rows: {len(df)}")
    return df


@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """
    Write DataFrame out locally as parquet file
    """
    path = Path(f"data/{color}/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")

    return path


@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS. Min 22:20"""
    gcs_block = GcsBucket.load("zoom-gcs") 
    gcs_block.upload_from_path(
        from_path=f"{path}",
        to_path=path
    )
    return


@flow(retries=3)
def etl_web_to_gcs() -> None:
    """Main ETL function"""
    color = "yellow"
    year = 2019
    month = 3
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path)



if __name__ == '__main__':
    etl_web_to_gcs()