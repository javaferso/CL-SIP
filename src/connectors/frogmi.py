import requests
import os
import json
from typing import Literal, Optional
from beartype import beartype
from utils.datetime import get_timedelta_days, get_current_date

auth_token  = os.getenv('FROGMI_AUTH_TOKEN')
company_uuid  = os.getenv('FROGMI_COMPANY_UUID')

class Api:
    def __init__(self):
        self.headers = {
                'Authorization':  auth_token,  
                'X-Company-UUID': company_uuid,
                'Content-Type': 'application/json'
                }  
    
    def post_activities(self, payload: dict) -> requests.models.Response:
        """
        Allows you take some template and publish to collect information.
        """
        
        self.payload = payload
        url = "https://api.frogmi.com/api/v3/tasks_management/activities" 

        return requests.request("POST", url, headers=self.headers, data=json.dumps(self.payload))
    
    def get_stores(self)-> requests.models.Response:
        """
        Get all Stores that your platform has access to.
        """
        url = "https://api.frogmi.com/api/v3/stores"

        return requests.request("GET", url, headers=self.headers)
    
    def get_task_management_results(self, activity_id: str, from_date: Optional[str]= get_current_date(as_string= True), to_date: Optional[str]= get_timedelta_days(days=-1, as_string= True),store_id: Optional[str]= None)-> requests.models.Response:
        """
        StoreBeat results represent the info gotten from users in other words the answer of Activities published.
        Events and Results has their own dates and those dates will be returned in the same timezone you request.

        Args:
            activity_id: Array with Activities ID
            from_date: Date from fetch results. Format ISO-8601
            to_date: Date to fetch results. Format ISO-8601
            store_id: The store ID.
        """
        
        self.activity_id = activity_id
        self.from_date = from_date
        self.to_date = to_date
        self.store_id = store_id

        url = f"https://api.frogmi.com/api/v3/tasks_management/results?filters[period][from]={self.from_date}&filters[period][to]={self.to_date}"
        
        if store_id:
            url += f'&[store]={store_id}'

        url += f'&filters[activity]={activity_id}&per_page=100'

        print(url)

        return requests.request("GET", url, headers=self.headers)
      
@beartype
def create_payload_from_dict(data: dict, type: Literal['task_general', 'task_storebeat', 'task_info', 'task_sku_info'])-> dict:

    payload_model = {
        'type': type,
        'attributes': {
            'name': '',
            'template_id': '',
            'accountable_area_id': '',
            'stores': '',
            'start_date': '',
            'end_date': '',
            'opportunity': {
                'value': '', 
                'currency_code': ''},
            'notification': '',
            'instructions': '',
            'external_id': '',
            'external_data': ''
        }
        
        }
    
    for key, value in data.items():
        payload_model['attributes'][key] = value

    return {'data': [payload_model]}


