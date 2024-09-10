import pandas as pd
import numpy as np
from typing import List, Any
from data.features import create_store_params_dict, get_products_list, validate_assersions, validate_production
from data.fetch import ( 
                        get_profile_matrix_data, 
                        get_dim_products_data,
                        get_dim_store_data,
                        get_stock_data,
                        get_prediction_data,
                        get_bakery_detailed_data,
                        get_roastery_detailed_data,
                        get_stock_data_from_bucket
                        )
from datetime import timedelta
from utils.datetime import (get_timedelta_days, get_timedelta_weeks)
from utils.strings import convert_int_to_char_str

def get_store_params_and_products_list(store_id: int):
    sip_dim_store_data = get_dim_store_data()
    
    store_params = create_store_params_dict(df = sip_dim_store_data, filter_by= 'store_id', store_id= store_id)
    products_list = get_products_list(store_params)
    
    return store_params, products_list

def get_products_to_produce(products_list: List[int]) -> pd.DataFrame:
    products_data = get_dim_products_data()

    return products_data.loc[products_data['sku'].isin(products_list)][['sku_name', 'sku', 'group_id', 'type']]

def get_bakery_and_roastery_data(products_to_produce, prediction_data) -> Any:
    merged_data = products_to_produce.merge(prediction_data, on='sku',how='left')

    if validate_assersions(merged_data):
        merged_filtered_data = merged_data.loc[merged_data['predictionday'] == merged_data['referenceday']]
        bakery = merged_filtered_data.loc[merged_filtered_data['type'] == 1]
        roastery = merged_filtered_data.loc[merged_filtered_data['type'] == 2]

        #stock_data = get_stock_data(query) # Query aún no está lista.
        stock_data = get_stock_data_from_bucket()

        bakery = bakery.merge(stock_data, on=['sku', 'predictionday'], how='left')
        roastery = roastery.merge(stock_data, on=['sku', 'predictionday'], how='left')

        validate_production(df=bakery)
        validate_production(df=roastery)

        return bakery, roastery, validate_production(df=bakery), validate_production(df=roastery)
    
def open_prediction(product_df, store_id) -> pd.DataFrame:
    profile_df = get_profile_matrix_data(store_id)

    products_unique_list = product_df['sku'].unique().tolist()

    return profile_df.loc[(profile_df['sku'].isin(products_unique_list)) & (profile_df['weekday']==pd.to_datetime(product_df['predictionday']).dt.weekday[0])]


def production_batches(df :pd.DataFrame, products :pd.DataFrame, predictions :pd.DataFrame, detail :pd.DataFrame, hourly_columns: list, bakery_batches: dict, roast_chiken_batches: dict) -> pd.DataFrame:
    # Variable to know which process will be evaluated
    classification_state = 0

    # Identify process, bakery type 1 and roast chiken type 2
    # global classification_state
    if not (df.loc[df['sku'].isin(products.loc[products['type']==1]['sku_sales'].unique().tolist())].empty):
        classification_state = 1
        df = df.loc[df['sku'].isin(products.loc[products['type']==1]['sku_sales'].unique().tolist())]
    elif not (df.loc[df['sku'].isin(products.loc[products['type']==2]['sku_sales'].unique().tolist())].empty):
        classification_state = 2
        df = df.loc[df['sku'].isin(products.loc[products['type']==2]['sku_sales'].unique().tolist())]
    
    # Create bins
    bin_edges = [i for i in range(0, 93, 3)]
    
    # Calculates the sum of peso for each number range in relation to the products column.
    df['intervalo_'] = pd.cut(df['number'], bins=bin_edges, right=True)
    df = df.groupby(['sku', 'intervalo_'])['weight'].sum().reset_index()
    df['weight'].fillna(0, inplace=True)

    # Pivot the table to get the calculations as columns
    df = df.pivot_table(index='sku', columns='intervalo_', values='weight', fill_value=0)

    # Renames columns according to interval schedule 
    df.columns = [7 + 0.5 * i for i in range(len(df.columns))]
    df.reset_index(inplace=True) 
    
    # Identify the process and perform a merge with the predictions
    if classification_state == 1:
        df = df.loc[df['sku'].isin(products.loc[products['type']==1]['sku_sales'].unique().tolist())]
        df = df.merge(predictions, on=['sku'], how='inner')
    elif classification_state == 2:
        df = df.loc[df['sku'].isin(products.loc[products['type']==2]['sku_sales'].unique().tolist())]
        df = df.merge(predictions, on=['sku'], how='inner')

    # Convert the values from percentages to target value per interval
    hourly_columns = [i/2 + 7 for i in range(30)]
    df[hourly_columns] = df[hourly_columns].multiply(df['finalforecast'], axis=0)
    
    # Evaluate process for establishing the columns to be used
    if classification_state == 1:
        keep_columns = ['sku', 'unit_weight', 'units_per_tray']
        bakery_columns = keep_columns + hourly_columns
        df = df.merge(detail, on = 'sku', how='left')[bakery_columns]
    elif classification_state == 2:
        keep_columns = ['sku', 'qty_accessories', 'qty_inputs_per_accessory']
        roast_chiken_columns = keep_columns + hourly_columns
        df = df.merge(detail, on = 'store_id', how='left')[roast_chiken_columns]
        
    # Generate production batches according to the defined schedule of batches
    production_batches = pd.DataFrame()
    production_batches['sku'] = df['sku']
    if classification_state == 1:
        for i in range(len(bakery_batches)):
            mask = (df.iloc[:, 3:].columns >= bakery_batches[i][0]) & (df.iloc[:, 3:].columns < bakery_batches[i][1])
            production_batches[f'Tanda {i+1}'] = pd.Series(np.ceil(np.ceil(df.loc[:, df.columns[3:][mask]].sum(axis=1).values*1000/df['Peso por unidad'].values)/df['Unidades por bandeja'].values))
    # In the case of roast chickens, if the prediction is less than 8, 8 will be defined as the production to be realized
    elif classification_state == 2:
        if int(np.ceil(df.iloc[:, 3:].sum(axis=1).values)) < 8:
            for i in range(len(roast_chiken_batches)):
                aux = 0
                if i==0:
                    aux = 8 
                production_batches[f'Tanda {i+1}'] = aux
        else:
            for i in range(len(roast_chiken_batches)):
                mask = (df.iloc[:, 3:].columns >= roast_chiken_batches[i][0]) & (df.iloc[:, 3:].columns < roast_chiken_batches[i][1])
                production_batches[f'Tanda {i+1}'] = int(np.ceil(np.ceil(df.loc[:, df.columns[3:][mask]].sum(axis=1).values)))    
                
    return df, production_batches

def create_bakery_to_mask(store_id: int, hourly_columns: list, bakery_batches: dict, roast_chiken_batches: dict):
    return get_transformed_data(store_id, hourly_columns, bakery_batches, roast_chiken_batches)


def set_rounded_values_per_sku(series: dict, baking_trays: int):
    sum_series = sum(series.values())
    if (sum_series <= baking_trays):
        return list(series.values())
    
    diff = abs(sum_series - baking_trays)
    variables = list(series.values())
    index_max = variables.index(max(variables))
    variables[index_max] -= diff
    return list({key: value for key, value in zip(series.keys(), variables)})


def get_transformed_data(store_id, hourly_columns, bakery_batches, roast_chiken_batches):

    store_params, products_list = get_store_params_and_products_list(store_id)
    
    products_to_produce = get_products_to_produce(products_list)    
    
    prediction_data = get_prediction_data(store_id)

    bakery, roastery, bakery_validator, roastery_validator  = get_bakery_and_roastery_data(products_to_produce, prediction_data)

    bakery_prediction   = open_prediction(bakery, store_id)
    roastery_prediction = open_prediction(roastery, store_id)

    dim_products = get_dim_products_data()
    bakery_detailed_data = get_bakery_detailed_data()
    roastery_detailed_data = get_roastery_detailed_data()


    df_bakery, bakery_batch = production_batches(bakery_prediction, dim_products, bakery, bakery_detailed_data, hourly_columns, bakery_batches, roast_chiken_batches)
    
    return df_bakery


def get_stock_data(store_id: int) -> pd.DataFrame:
    """
    Get the store data for a given store ID.

    Args:
        store_id: The store ID.
    """

    products_data = get_dim_products_data()
    
    # Lineas agregadas, se debe modificar get_dim_products_data() para incorporar los siguientes productos
    elementos_agregar = [612746, 612745, 665561, 636110, 649664, 655753, 649665, 642541]
    nuevo_df = pd.DataFrame({'sku': elementos_agregar})
    products_data = pd.concat([products_data, nuevo_df], ignore_index=True)
    
    products_unique_array = products_data['sku'].astype(str).unique()
    products_unique_list = [convert_int_to_char_str(sku, 18) for sku in products_unique_array]
    products_unique_str  = ", ".join([f"'{value}'" for value in products_unique_list])

    end_date    = get_timedelta_days(1)  
    start_date  = get_timedelta_weeks(4) 

    while start_date.weekday() != end_date.weekday():  
        start_date -= timedelta(days=1)

    str_store_id = convert_int_to_char_str(store_id, 4)

    sales_query = f"""
        SELECT 
          DS.STORE_ID, 
          DD.DATE_VALUE, 
          DT.TIME_VALUE, 
          DSKU.SKU_PRODUCT, 
          FIT.WGHT_ITM AS PRODUCT_WEIGHT,
          FIT.NBR_PD_ITM AS PRODUCTS_QUANTITY,
          (PARSE_NUMERIC(DP.CONT_CONV_UMB) / PARSE_NUMERIC(DP.DENOM_UMB))*(FIT.NBR_PD_ITM) AS SALES_QUANTITY 
        FROM `cl-data-procesada.AMP.FACT_ITM_TXN` FIT
          INNER JOIN  `cl-data-procesada.AMP.DIM_DATE` DD USING (DATE_KEY) 
          INNER JOIN  `cl-data-procesada.AMP.DIM_TIME` DT USING (TIME_KEY) 
          INNER JOIN  `cl-data-procesada.AMP.DIM_STORE_HIERARCHY` DSH USING (STORE_KEY) 
          INNER JOIN  `cl-data-procesada.AMP.DIM_STORE` DS USING (STORE_KEY) 
          INNER JOIN  `cl-data-procesada.AMP.DIM_PRODUCT_HIERARCHY` DPH ON PRODUCT_KEY=PRODUCT_KEY_1 
          INNER JOIN  `cl-data-procesada.AMP.DIM_SKU_HIERARCHY` DSKU USING (SKU_KEY) 
          INNER JOIN  `cl-data-procesada.AMP.DIM_PRODUCT` DP USING (PRODUCT_KEY) 
        WHERE FIT.DATE_VALUE >= '{start_date}' AND FIT.DATE_VALUE <= '{end_date}'
          AND DS.STORE_ID = '{str_store_id}'
          AND DSKU.SKU_PRODUCT IN ({products_unique_str})
    """
    
    sales_data = get_sales_data(sales_query) 
    bakery_production_data = get_bakery_detailed_data()
    
    
    result = sales_data.merge(bakery_production_data, on='sku_product', how='left')
    result = result.loc[result['product_weight']>=0] #
    result['sold_units'] = result['product_weight']/result['unit_weight']
    result['sold_units'] = result['sold_units'].fillna(0) #
    result['sold_units'] = result['sales_quantity'] + result['sold_units']
    
    # New line
    result = result.drop(['product_nm', 'unit_weight', 'units_per_tray', 'baking_minutes'], axis=1)
    
    return result
