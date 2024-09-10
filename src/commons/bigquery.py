import pandas as pd
from google.cloud import bigquery
from typing import Optional

def read_data_from_bigquery(query:str, project:Optional[str] = "cl-sip-dev", location:Optional[str] = 'US') -> pd.DataFrame:
    """Read data from BigQuery

    Args:
    query: query to execute

    """
    # Crea una conexión con BigQuery.
    client = bigquery.Client(location= location, project= project)

    return client.query(query, location= location).to_dataframe()