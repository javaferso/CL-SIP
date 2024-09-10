import numpy as np
import pandas as pd
from typing import List
from data.preprocessing import create_bakery_to_mask, set_rounded_values_per_sku

def create_matrix_of_possibilities(series_family: dict, baking_trays: int):
    if len(series_family) == 2:
        if len(series_family['s1']) == 0 or len(series_family['s2']) == 0 or len(series_family['s1']) != len(series_family['s2']):
            raise ValueError("Las series no pueden estar vacías ni tener diferentes dimensiones")
        # Crear un arreglo tridimensional para almacenar los resultados
        shape = (len(series_family['s1']), len(series_family['s2']))
        possibilities = np.empty(shape, dtype=float)
        
        for i, val1 in enumerate(series_family['s1']):
            for j, val2 in enumerate(series_family['s2']):
                possibilities[i, j] = val1 + val2 
                
    elif len(series_family) == 3:
        if len(series_family['s1']) == 0 or len(series_family['s2']) == 0 or len(series_family['s3']) == 0 or len(series_family['s1']) != len(series_family['s2']) or len(series_family['s2']) != len(series_family['s3']):
            raise ValueError("Las series no pueden estar vacías ni tener diferentes dimensiones")
        # Crear un arreglo tridimensional para almacenar los resultados
        shape = (len(series_family['s1']), len(series_family['s2']), len(series_family['s3']))
        possibilities = np.empty(shape, dtype=float)
        
        for i, val1 in enumerate(series_family['s1']):
            for j, val2 in enumerate(series_family['s2']):
                for k, val3 in enumerate(series_family['s3']):
                    possibilities[i, j, k] = val1 + val2 + val3
      
    diff = possibilities - (baking_trays - 1)
    tolerance = 10 # evaluar la tolerancia a medida que se va a iterando el arreglo, es posible que se debe ampliar el numero
    near_indexes = np.argwhere((diff <= tolerance) & (diff>0))
    while len(near_indexes) == 0:
        baking_trays = baking_trays - 1
        diff = possibilities - (baking_trays)
        near_indexes = np.argwhere((diff <= tolerance) & (diff>0))
        
    
    return near_indexes[0]

def get_series_family(series: dict, matrix_possibilities: dict):
    if len(matrix_possibilities) < 2 or len(matrix_possibilities) > 3:
        raise ValueError("El diccionario debe contener 2 o 3 series")
    if len(series) < 2 or len(series) > 3:
        raise ValueError("El diccionario debe contener 2 o 3 series")
    if len(matrix_possibilities) != len(series):
        raise ValueError("El diccionario de posibilidades debe tener la misma cantidad de series que el diccionario de series")
    
    
    series_1 = np.ceil(series['s1'].iloc[matrix_possibilities[0]])
    series_2 = np.ceil(series['s2'].iloc[matrix_possibilities[1]])
    series_family = {
        's1': series_1,
        's2': series_2
    }
    if len(matrix_possibilities) == 3:
        series_3 = np.ceil(series['s3'].iloc[matrix_possibilities[2]])
        series_family = {
            's1': series_1,
            's2': series_2,
            's3': series_3
        }
    return series_family

def variables_setup(planning_bakery :pd.DataFrame, mask :np.ndarray, bakery_families :dict, hourly_columns :List[float]):
    
    planning_bakery['familia'] = 0
    for key, value in bakery_families.items():
        planning_bakery.loc[planning_bakery['sku'].isin(value), 'familia'] = key

    planning_bakery[hourly_columns] = ((planning_bakery[hourly_columns].multiply(1000, axis=0)).div(planning_bakery['unit_weight'], axis=0)).div(planning_bakery['units_per_tray'], axis=0)
    keep_columns = ['sku', 'familia']
    planning_bakery_columns = keep_columns + hourly_columns
    planning_bakery = planning_bakery[planning_bakery_columns].sort_values(by='familia')
    
    s1_f1 = planning_bakery.loc[planning_bakery['familia']==1].loc[:, planning_bakery.columns[2:][mask]].cumsum(axis=1).reset_index(drop=True).loc[0]
    s2_f1 = planning_bakery.loc[planning_bakery['familia']==1].loc[:, planning_bakery.columns[2:][mask]].cumsum(axis=1).reset_index(drop=True).loc[1]
    s3_f1 = planning_bakery.loc[planning_bakery['familia']==1].loc[:, planning_bakery.columns[2:][mask]].cumsum(axis=1).reset_index(drop=True).loc[2]

    s1_f2 = planning_bakery.loc[planning_bakery['familia']==2].loc[:, planning_bakery.columns[2:][mask]].cumsum(axis=1).reset_index(drop=True).loc[0]
    s2_f2 = planning_bakery.loc[planning_bakery['familia']==2].loc[:, planning_bakery.columns[2:][mask]].cumsum(axis=1).reset_index(drop=True).loc[1]

    s1_f3 = planning_bakery.loc[planning_bakery['familia']==3].loc[:, planning_bakery.columns[2:][mask]].cumsum(axis=1).reset_index(drop=True).loc[0]
    s2_f3 = planning_bakery.loc[planning_bakery['familia']==3].loc[:, planning_bakery.columns[2:][mask]].cumsum(axis=1).reset_index(drop=True).loc[1]

    families = {
        'f1': {
            's1': s1_f1,
            's2': s2_f1,
            's3': s3_f1
        },
        'f2': {
            's1': s1_f2,
            's2': s2_f2
        },
        'f3': {
            's1': s1_f3,
            's2': s2_f3
        }
    }
    return planning_bakery, families


def get_vector_family_champion(**kwargs) -> str:
    return (min(kwargs, key=kwargs.get))

def family_winner_second_phase(families: dict, vectors_champion: dict, baking_trays: int) -> str:
    sum_series_by_family = {}
    for key, values in families.items():
        sum_series_by_family[key] = sum(values)
    
    while (sum_series_by_family['f1']) < baking_trays and (sum_series_by_family['f2']) < baking_trays and (sum_series_by_family['f3']) < baking_trays:
        baking_trays -= 1
    
    if (sum_series_by_family['f1']) >= baking_trays and (sum_series_by_family['f2']) >= baking_trays and (sum_series_by_family['f3']) >= baking_trays:
        family_winner = get_vector_family_champion(f1 = vectors_champion['f1'], f2 = vectors_champion['f2'], f3 = vectors_champion['f3'])
    elif (sum_series_by_family['f1']) >= baking_trays and (sum_series_by_family['f2']) >= baking_trays:
        family_winner = get_vector_family_champion(f1 = vectors_champion['f1'], f2 = vectors_champion['f2'])
    elif (sum_series_by_family['f1']) >= baking_trays and (sum_series_by_family['f3']) >= baking_trays:
        family_winner = get_vector_family_champion(f1 = vectors_champion['f1'], f3 = vectors_champion['f3'])
    elif (sum_series_by_family['f2']) >= baking_trays and (sum_series_by_family['f3']) >= baking_trays:
        family_winner = get_vector_family_champion(f2 = vectors_champion['f2'], f3 = vectors_champion['f3'])
    elif (sum_series_by_family['f1']) >= baking_trays:
        family_winner = 'f1'
    elif (sum_series_by_family['f2']) >= baking_trays:
        family_winner = 'f2'
    elif (sum_series_by_family['f3']) >= baking_trays:
        family_winner = 'f3'

    return family_winner

def modification_of_winning_vector_values(General_info: pd.DataFrame, family_vector: int, baking_trays: int, series: dict):
    family_info = General_info.loc[General_info["familia"] == family_vector].reset_index().drop("index", axis=1)

    if len(series) < 2 or len(series) > 3:
        raise ValueError("El diccionario debe contener 2 o 3 series")

    if len(series) == 2:
        sku_0 = family_info["sku"][0]
        sku_1 = family_info["sku"][1]
        s1_value = series['s1']['value'] # args[0]
        s2_value = series['s2']['value'] # args[1]
        s1_original_value = series['s1']['original_value'] # args[2]
        s2_original_value = series['s2']['original_value'] # args[3]
        if (s1_value)>0 or (s2_value)>0:
            if (s1_value + s2_value) != baking_trays and (s2_original_value - s2_value).apply(lambda x: max(0, x)).gt(0).any() or (s1_original_value - s1_value).apply(lambda x: max(0, x)).gt(0).any():
                if (((s1_original_value - s1_value).apply(lambda x: max(0, x))).sum()) >= (((s2_original_value - s2_value).apply(lambda x: max(0, x))).sum()): 
                    new_value = np.ceil(((s1_original_value - s1_value).apply(lambda x: max(0, x))).sum()) 
                    diff = abs((s1_value + s2_value) - baking_trays) 
                    if new_value > diff: 
                        new_value = diff
                        s1_value = (s1_value) + new_value  
                    else:
                        s1_value = (s1_value) + new_value  
                else:
                    new_value = np.ceil(((s2_original_value - s2_value).apply(lambda x: max(0, x))).sum())
                    diff = abs((s1_value + s2_value) - baking_trays)  
                    if new_value > diff:
                        new_value = diff
                        s2_value = (s2_value) + new_value  
                    else:
                        s2_value = (s2_value) + new_value  
            instructions = f"{s1_value} bandeja de {sku_0} y {s2_value} bandeja de {sku_1}" if s1_value > 0 and s2_value > 0 else \
                        f"{s1_value} bandeja de {sku_0}" if s1_value > 0 else \
                        f"{s2_value} bandeja de {sku_1}" if s2_value > 0 else ""
            s1_original_value = (s1_original_value - s1_value).apply(lambda x: max(0, x))
            s2_original_value = (s2_original_value - s2_value).apply(lambda x: max(0, x))
            return instructions, [s1_original_value, s2_original_value]
        else:
            return '', [s1_original_value, s2_original_value]
    
    if len(series) == 3:
        sku_0 = family_info["sku"][0]
        sku_1 = family_info["sku"][1]
        sku_2 = family_info["sku"][2]
        s1_value = series['s1']['value'] # args[0]
        s2_value = series['s2']['value'] # args[1]
        s3_value = series['s3']['value'] # args[2]
        s1_original_value = series['s1']['original_value'] # args[3]
        s2_original_value = series['s2']['original_value'] # args[4]
        s3_original_value = series['s3']['original_value'] # args[5]
        if (s1_value)>0 or (s2_value) or (s3_value)>0:
            if (s1_value + s2_value) != baking_trays and ((s1_original_value - s1_value).apply(lambda x: max(0, x)).gt(0).any() or (s2_original_value - s2_value).apply(lambda x: max(0, x)).gt(0).any() or (s3_original_value - s3_value).apply(lambda x: max(0, x)).gt(0).any()):
                if ((((s1_original_value - s1_value).apply(lambda x: max(0, x))).sum()) >= (((s2_original_value - s2_value).apply(lambda x: max(0, x))).sum())) and ((((s1_original_value - s1_value).apply(lambda x: max(0, x))).sum()) >= (((s3_original_value - s3_value).apply(lambda x: max(0, x))).sum())):
                    new_value = np.ceil(((s1_original_value - s1_value).apply(lambda x: max(0, x))).sum()) 
                    diff = abs((s1_value + s2_value + s3_value) - baking_trays) 
                    if new_value > diff: 
                        new_value = diff
                        s1_value = (s1_value) + new_value  
                    else:
                        s1_value = (s1_value) + new_value  
                if ((((s2_original_value - s2_value).apply(lambda x: max(0, x))).sum()) >= (((s1_original_value - s1_value).apply(lambda x: max(0, x))).sum())) and ((((s2_original_value - s2_value).apply(lambda x: max(0, x))).sum()) >= (((s3_original_value - s3_value).apply(lambda x: max(0, x))).sum())):
                    new_value = np.ceil(((s2_original_value - s2_value).apply(lambda x: max(0, x))).sum()) 
                    diff = abs((s1_value + s2_value + s3_value) - baking_trays) 
                    if new_value > diff: 
                        new_value = diff
                        s2_value = (s2_value) + new_value  
                    else:
                        s2_value = (s2_value) + new_value  
                if ((((s3_original_value - s3_value).apply(lambda x: max(0, x))).sum()) >= (((s1_original_value - s1_value).apply(lambda x: max(0, x))).sum())) and ((((s3_original_value - s3_value).apply(lambda x: max(0, x))).sum()) >= (((s2_original_value - s2_value).apply(lambda x: max(0, x))).sum())):
                    new_value = np.ceil(((s3_original_value - s3_value).apply(lambda x: max(0, x))).sum()) 
                    diff = abs((s1_value + s2_value + s3_value) - baking_trays) 
                    if new_value > diff: 
                        new_value = diff
                        s3_value = (s3_value) + new_value  
                    else:
                        s3_value = (s3_value) + new_value  
        
            instructions = f"{s1_value} bandeja de {sku_0} y {s2_value} bandeja de {sku_1} y {s3_value} bandeja de {sku_2}" if s1_value > 0 and s2_value and s3_value > 0 else \
                        f"{s1_value} bandeja de {sku_0} y {s2_value} bandeja de {sku_1}" if s1_value > 0 and s2_value > 0 else \
                        f"{s1_value} bandeja de {sku_0} y {s3_value} bandeja de {sku_2}" if s1_value > 0 and s3_value > 0 else \
                        f"{s2_value} bandeja de {sku_1} y {s3_value} bandeja de {sku_2}" if s2_value > 0 and s3_value > 0 else \
                        f"{s1_value} bandeja de {sku_0}" if s1_value > 0 else \
                        f"{s2_value} bandeja de {sku_1}" if s2_value > 0 else \
                        f"{s3_value} bandeja de {sku_2}" if s3_value > 0 else ""
            s1_original_value = (s1_original_value - s1_value).apply(lambda x: max(0, x))
            s2_original_value = (s2_original_value - s2_value).apply(lambda x: max(0, x))
            s3_original_value = (s3_original_value - s3_value).apply(lambda x: max(0, x))
            return instructions, [s1_original_value, s2_original_value, s3_original_value]
        else:
            return '', [s1_original_value, s2_original_value, s3_original_value]

def create_production_plan(store_id: int, bakery_batches: dict, baking_trays: int, bakery_families: dict, hourly_columns: list, roast_chiken_batches: dict):
    production_plan  = pd.Series(dtype='object')

    df_bakery_to_mask = create_bakery_to_mask(store_id, hourly_columns, bakery_batches, roast_chiken_batches)
    
    for i in range(len(bakery_batches)):
        mask = (df_bakery_to_mask.iloc[:, 3:].columns >= bakery_batches[i][0]) & (df_bakery_to_mask.iloc[:, 3:].columns < bakery_batches[i][1])

        planning_bakery, families = variables_setup(df_bakery_to_mask.copy(), mask, bakery_families, hourly_columns)
        
        for n in range((bakery_batches[i][1] - bakery_batches[i][0]) * 2):
            fam1_coordenates = create_matrix_of_possibilities(series_family=families['f1'], baking_trays=baking_trays)
            fam2_coordenates = create_matrix_of_possibilities(series_family=families['f2'], baking_trays=baking_trays)
            fam3_coordenates = create_matrix_of_possibilities(series_family=families['f3'], baking_trays=baking_trays)
            
            
            series = {
                'f1': get_series_family(families['f1'], fam1_coordenates),
                'f2': get_series_family(families['f2'], fam2_coordenates),
                'f3': get_series_family(families['f3'], fam3_coordenates)
            }

            # Redondeo de valores según coordenadas
            series_by_family = {
                'f1': set_rounded_values_per_sku(series['f1'],  baking_trays),
                'f2': set_rounded_values_per_sku(series['f2'],  baking_trays),
                'f3': set_rounded_values_per_sku(series['f3'],  baking_trays)
            }
            
            # Vector campeon por familia
            vectors_champion_by_family = {
                'f1': fam1_coordenates.sum(),
                'f2': fam2_coordenates.sum(),
                'f3': fam3_coordenates.sum()
            }

            #Logica adicional para identificar vector ganador, incorporar el total de bandejas a hornear
            family_winner = family_winner_second_phase(series_by_family, vectors_champion_by_family, baking_trays)

            first_variable_name = f's1_{family_winner}'
            second_variable_name = f's2_{family_winner}'
            first_variable = series_by_family[family_winner][0]
            second_variable = series_by_family[family_winner][1]
            first_original_value = families[family_winner]['s1']
            second_original_value = families[family_winner]['s2']
            series_to_process = {
                's1': {
                    'variable_name': first_variable_name,
                    'original_value': first_original_value,
                    'value': first_variable,
                },
                's2': {
                    'variable_name': second_variable_name,
                    'original_value': second_original_value,
                    'value': second_variable,
                }
            }
            
            if family_winner == 'f1':
                third_variable_name = f's3_{family_winner}'
                third_variable = series_by_family[family_winner][2]
                third_original_value = families[family_winner]['s3']
                series_to_process['s3'] = {
                    'variable_name': third_variable_name,
                    'original_value': third_original_value,
                    'value': third_variable
                }
            
            instructions, new_values = modification_of_winning_vector_values(planning_bakery, int(family_winner[-1:]), baking_trays, series_to_process)

            first_original_value = new_values[0]
            second_original_value = new_values[1]
            families[family_winner]['s1'] = new_values[0]
            families[family_winner]['s2'] = new_values[1]
            if (len(new_values) >= 3):
                third_original_value = new_values[2]
                families[family_winner]['s3'] = new_values[2]
            
            production_plan = production_plan._append(pd.Series([instructions]), ignore_index=True)
    return production_plan
