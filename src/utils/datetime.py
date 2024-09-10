from datetime import datetime, timedelta
from typing import Optional, Union

def get_current_date(as_string:Optional[bool]=False) -> Union[datetime.date, str]:
    """Generates current date as date or as string.

    Args:
    as_string: expected output format. 
        Defaults to False.
        
    """
    if as_string:
        date = datetime.now().date().strftime("%Y-%m-%d")
    else:
        date = datetime.now().date()
    return date

def get_timedelta_days(days:int, as_string:Optional[bool]=False) -> Union[datetime.date, str]:
    """
    Calculates the date a certain number of days before the current date.

    Args:
        days : The number of days to subtract from the current date.
        as_string: Whether to return the date as a string or as a datetime object.
            Defaults to False.
    """
    if as_string:
        date = (datetime.now().date() - timedelta(days=days)).strftime("%Y-%m-%d")
    else:
        date = datetime.now().date() - timedelta(days=days)  
    return date

def get_timedelta_weeks(weeks:int) -> datetime.date:
    """
    Calculates the date a certain number of weeks before the current date.

    Args:
        weeks: The number of weeks to subtract from the current date.

    """
    return datetime.now().date() - timedelta(weeks=weeks)
