import pydantic 
from datetime import date, time, datetime, timedelta
from typing import Union
import pandas as pd
from typing import List, TypeVar, Optional, Any

B = TypeVar('B', bound=pydantic.BaseModel)

class BaseModel(pydantic.BaseModel):
  pass

class BakeryProduction(BaseModel):
  sku_product: int
  product_nm: str
  unit_weight: int
  units_per_tray: int
  baking_minutes: int

class RoasteryProduction(BaseModel):
  store_id: int
  oven_model: str
  oven_brand: str
  operative_ovens: int
  energy_source: str
  oven_type: str
  accessories_type: str
  qty_accessories: int
  qty_inputs_per_accessory: int
  maintainers: int
  qty_maintainers: int

class DimStore(BaseModel):
  store_id: int
  store_name: str
  surface: int
  format_id: int
  commune: str
  opening_date: date
  is_active: bool
  sanitary_resolution: bool
  process_room: bool
  frogmi_id: str
  frogmi_is_active: bool
  baking_trays_for_bakery: int

  @pydantic.field_validator('opening_date', mode='before')
  def date_str_parse(cls, value) -> date:
      return pd.to_datetime(value).date()

class DimProducts(BaseModel):
  sku: int
  sku_name: str
  group_id: int
  group_name: str
  category_name: str
  category_id: int
  unit_measurement: str
  type: int
  sku_sales: int


class Predictions(BaseModel):
  sku: int = pydantic.Field(alias='plu_sap60')
  store_id: int
  predictionday: date
  referenceday: date
  finalforecast: float

  @pydantic.field_validator('predictionday', 'referenceday', mode='before')
  def date_str_parse(cls, value) -> date:
      return pd.to_datetime(value).date()

class Stock(BaseModel):
  sku: int
  predictionday: date = pydantic.Field(alias='fecha_medicion_inventario')
  stock_umb: int

  @pydantic.field_validator('predictionday')
  def date_timedelta(cls, value: date) -> date:
      return pd.to_datetime(value).date() + timedelta(days=1)  


class ProfileMatrix(BaseModel):
  store_id: int
  sku_product: int
  weight: int
  weekday: int
  week: int
  interval: str
  records_sum: float
  number: Optional[Any] = None
  day_total: float
  weight: float


class FrogmiAttributes(BaseModel):
  name: str
  template_id: Optional[str] = None
  accountable_area_id: Optional[str] = '0d3616d8-fece-49ec-9c55-260d91c5ebe2'
  viewer_accountable_area_id : Optional[str] = None
  stores: List[str]
  start_date: datetime
  end_date: datetime
  opportunity: Optional[dict] = {'value': 0, 'currency_code': 'CLP'}
  external_id: Optional[str] = None
  instructions: str
  external_data: Optional[List[dict]] = None
  notification: Optional[List[str]] = None

  @pydantic.field_validator('start_date')
  def validate_start_date(cls, value: datetime) -> str:
    return value.strftime('%Y-%m-%dT%H:%M:%SZ')

  @pydantic.field_validator('end_date')
  def validate_end_date(cls, value: datetime) -> str:
    return value.strftime('%Y-%m-%dT%H:%M:%SZ')

  @pydantic.field_validator('opportunity')
  def validate_opportunity_value(cls, value: dict) -> dict:
    for key in value.keys():
      if key == 'value':
        assert isinstance(value[key], int)
    return value



class DataFrameStandarization:
    @staticmethod
    def standarize_schema(df: pd.DataFrame, model: B) -> pd.DataFrame:
        """ 
        Standarized dataframe based on a schema model 
        """
        data_modeled : List[model]  = [model(**item).model_dump() for item in df.to_dict(orient='records')]

        return pd.DataFrame(data_modeled) 
    
    @staticmethod
    def standarize_dict(dict: dict, model: B) -> dict:
        """ 
        Standarized dataframe based on a schema model 
        """
        data_modeled : List[model] = model(**dict).model_dump()

        return data_modeled
       
    
