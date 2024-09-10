import pandas as pd
from google.cloud import storage
from typing import Literal, Optional
from utils.datetime import get_current_date
from beartype import beartype

@beartype
def read_from_gcs(bucket_name: str, filename: str, format: Literal['csv','parquet'], prefix: Optional[str]=None) -> pd.DataFrame:
    """Reads a file from a Google Cloud Storage bucket.

    Args:
    bucket_name: Google Cloud Storage bucket.
    filename: name of the file to read.
    format: format of the file to read. Supported formats are 'csv' and 'parquet'.
    prefix: path inside the bucket to get fhe file.
        Defaults = None
    """

    if prefix:
        path = f'gs://{bucket_name}/{prefix}/{filename}.{format}'
    else:
        path = f'gs://{bucket_name}/{filename}.{format}'
    
    print(path)

    if format == 'csv':
        dataframe = pd.read_csv(path)
    elif format == 'parquet':
        dataframe = pd.read_parquet(path)
    else:
        raise ValueError('Unsupported format: {}'.format(format))
    
    return dataframe[[col for col in dataframe.columns if 'Unnamed' not in col]]

@beartype
def export_dataframe_to_gcs(dataframe: pd.DataFrame, bucket_name: str, prefix: str, filename: str, format: Literal['csv','parquet'], partitioned: Optional[bool]=False) -> None:
    """Exports a Pandas DataFrame to a Google Cloud Storage bucket.

    Args:
    dataframe: Pandas DataFrame to export.
    bucket_name: name of the Google Cloud Storage bucket.
    filename: name of the file to export to.
    format: format of the exported file. Supported formats are 'csv' and 'parquet'.
    prefix: path inside the bucket to get fhe file.
        Defaults = None
    """

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    if partitioned:
        year  = get_current_date().year
        month = get_current_date().month
        day   = get_current_date().day

        partitioned_path = f"{prefix}/{year}/{month}/{day}"
    else:
        partitioned_path = prefix
    
    blob = bucket.blob(f"{partitioned_path}/{filename}.{format}")

    if format == 'csv':
        blob.upload_from_string(dataframe.to_csv(index=False))
    elif format == 'parquet':
        blob.upload_from_string(dataframe.to_parquet(index=False, compression='snappy'))
    else:
        raise ValueError('Unsupported format: {}'.format(format))
    


