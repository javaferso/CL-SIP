
from commons.cloud_storage import read_from_gcs
from commons.bigquery import read_data_from_bigquery
from commons.cloud_sql import CloudSQL
import pandas as pd
from utils.strings import headers_to_lower
from utils.pydantic_BaseModel import Predictions, DataFrameStandarization, DimStore, DimProducts, Stock, ProfileMatrix, BakeryProduction, RoasteryProduction
import os

sip_bucket =                os.getenv('SIP_BUCKET')                          
stock_filename =            os.getenv('STOCK_FILENAME')
dim_bakery_producction  =   os.getenv('DIM_BAKERY_PRODUCTION')
dim_products_filename  =    os.getenv('DIM_PRODUCTS_FILENAME')
dim_store_filename  =       os.getenv('DIM_STORE_FILENAME')
prediction_filename  =      os.getenv('PREDICTION_FILENAME')
profile_matrix_filename  =  os.getenv('PROFILE_MATRIX_FILENAME')
dim_roastery_production  =  os.getenv('DIM_ROASTERY_PRODUCTION')
bakery_detailed_filename = os.getenv('BAKERY_DETAILED_FILENAME')

def get_dim_store_data()-> pd.DataFrame:
    """Generate a Standarized Pandas Dataframe with lower headers from dim_store dimensional data allocated in Cloud Storage """
    sip_dim_store_data = read_from_gcs(bucket_name= sip_bucket, filename= dim_store_filename, format='csv', prefix='SOURCES')
    sip_dim_store_data.columns = headers_to_lower(sip_dim_store_data)
    print(sip_dim_store_data)
    return DataFrameStandarization.standarize_schema(sip_dim_store_data, DimStore)

def get_stock_data(query: str)-> pd.DataFrame:
    stock_data = read_data_from_bigquery(query)
    stock_data.columns = headers_to_lower(stock_data)
    
    return DataFrameStandarization.standarize_schema(stock_data, Stock)

def get_stock_data_from_bucket()-> pd.DataFrame:
    """Generate a Standarized Pandas Dataframe with lower headers from dim_store dimensional data allocated in Cloud Storage """
    sip_dim_stock_data = read_from_gcs(bucket_name= sip_bucket, filename= stock_filename, format='csv', prefix='SOURCES')
    sip_dim_stock_data.columns = headers_to_lower(sip_dim_stock_data)
    return DataFrameStandarization.standarize_schema(sip_dim_stock_data, Stock)

def get_dim_products_data()-> pd.DataFrame:
    """Generate a Standarized Pandas Dataframe with lower headers from dim_products dimensional data allocated in Cloud Storage """
    products_data = read_from_gcs(bucket_name= sip_bucket, filename= dim_products_filename, format='csv', prefix='SOURCES')
    products_data.columns = headers_to_lower(products_data)
    return DataFrameStandarization.standarize_schema(products_data, DimProducts)

def get_prediction_data(store_id: int) -> pd.DataFrame:
    """Generate a Standarized Pandas Dataframe with lower headers from Luminate server data allocated in Cloud SQL
    
    Args:
    store_id: store id to get information from   
    """
    predictions_data = CloudSQL().retrieve(f"SELECT * FROM public.luminate WHERE store_id = '{store_id}'")
    predictions_data.columns = headers_to_lower(predictions_data)
    return DataFrameStandarization.standarize_schema(predictions_data, Predictions)    

def get_profile_matrix_data(store_id: int)-> pd.DataFrame:
    """Generate a Standarized Pandas Dataframe with lower headers from Profile Matrix data allocated in Big Query
    
    Args:
    store_id: store id to get information from    
    """
    profile_matrix_data = read_from_gcs(bucket_name= sip_bucket, filename= profile_matrix_filename, format='csv', prefix=f'TRANSFORMED/{store_id}')
    profile_matrix_data.columns = headers_to_lower(profile_matrix_data)
    return DataFrameStandarization.standarize_schema(profile_matrix_data, ProfileMatrix)

def get_bakery_detailed_data()-> pd.DataFrame:
    """Generate a Standarized Pandas Dataframe with lower headers from bakery dimensional data allocated in Cloud Storage"""
    bakery_detailed_data = read_from_gcs(bucket_name= sip_bucket, filename= dim_bakery_producction, format='csv', prefix='SOURCES')
    bakery_detailed_data.columns = headers_to_lower(bakery_detailed_data)

    return DataFrameStandarization.standarize_schema(bakery_detailed_data, BakeryProduction)

def get_roastery_detailed_data()-> pd.DataFrame:
    """Generate a Standarized Pandas Dataframe with lower headers from roastery dimensional data allocated in Cloud Storage"""
    roastery_detailed_data = read_from_gcs(bucket_name= sip_bucket, filename= dim_roastery_production, format='csv', prefix='SOURCES')
    roastery_detailed_data.columns = headers_to_lower(roastery_detailed_data)

    return DataFrameStandarization.standarize_schema(roastery_detailed_data, RoasteryProduction)

def get_stock_data(query)-> pd.DataFrame:
    """Generate a Standarized Pandas Dataframe with lower headers from Stock data allocated in Big Query
    
    Args:
    query: query to execute    
    """
    stock_data = read_data_from_bigquery(query)
    stock_data.columns = headers_to_lower(stock_data)

    return DataFrameStandarization.standarize_schema(stock_data, Stock)






