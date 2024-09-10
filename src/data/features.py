import pandas as pd
from typing import List
from datetime import timedelta


def create_store_params_dict(df:pd.DataFrame, filter_by: str, store_id: int) -> dict:

    filtered_df = df.loc[df[filter_by] == store_id]
    headers = filtered_df.columns

    values = filtered_df.values[0]

    return dict(zip(headers, values))

def get_products_list(store_params) -> List[int]:
    if store_params['sanitary_resolution'] and store_params['process_room']:
        print('Se puede producir cualquier sku, pero se utiliza Pollo fresco y Pollo congelado')
        productos = [858091, 571581, 638484, 626218, 632861, 638771, 966668, 966669, 966670, 966679]
    elif not store_params['sanitary_resolution'] and store_params['process_room']:
        print('Solo se puede producir pollo pre-horneado')
        productos = [655130, 626218, 632861, 638771, 966668, 966669, 966670, 966679]
    else:
        productos = []
        print('No se puede producir')

    return productos

def validate_assersions(df: pd.DataFrame) -> bool:
    validator = False
    try:
        # Validamos que cada producto cuente con prediccion para el procesamiento de hoy, en caso de activarse el assert, capturaremos la informaciÃ³n del dÃ­a anterior de BY almacena en un Bucket
        assert df.loc[df['predictionday'] == df['referenceday']]['finalforecast'].apply(lambda x: isinstance(x, (int, float))).all(), f"La columna FinalForecast contiene valores no numÃ©ricos en el DataFrame."

        # Almacenaremos una semana de predicciones en un bucket a manera de backup, se realizarÃ¡ una validaciÃ³n para comprobar la integridad de los datos
        assert df.loc[df['predictionday']<= (df['referenceday'] + timedelta(weeks=1))]['finalforecast'].apply(lambda x: isinstance(x, (int, float))).all(), f"La columna FinalForecast contiene valores no numÃ©ricos en el DataFrame, no se podrÃ¡ almacenar informaciÃ³n de backup."
        '''
        Se deberÃ¡ crear la lÃ³gica de almacenamiento en el bucket para las predicciones, como tambiÃ©n el borrado automÃ¡tico (sin definiciÃ³n a historia a mantener aÃºn)
        '''
        validator = True

    except AssertionError as e:
        print(e)

    return validator

def validate_production(df: pd.DataFrame) -> bool:
    validator = False
    try:
        assert (df['stock_umb'] >= df['finalforecast']).all()
        validator = True
    except AssertionError as e:
        # Se debe almacenar en algÃºn lugar los productos que no tenÃ­an suficiente stock, es parte del checklist
        df.loc[df['finalforecast'] > df['stock_umb'], 'finalforecast'] = df['stock_umb']
        print('Se ajustÃ³ el valor objetivo de producciÃ³n en base a capacidad real de stock')

    return validator
