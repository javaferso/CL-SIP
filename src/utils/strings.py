import pandas as pd

def headers_to_lower(dataframe: pd.DataFrame) -> list:
    """
    Change all the headers to lower
    """
    return [col.lower() for col in dataframe.columns]

def convert_int_to_char_str(integer: str, chars : int) -> str:
    """Converts an integer to a 4-character string.

    Args:
    integer: The integer to convert.

    Returns:
    A N-character string representation of the integer.
    """
    if isinstance(integer, int):
        integer_str = str(integer)
    else:
        integer_str = integer
    
    if len(integer_str) < chars:
        integer_str = integer_str.zfill(chars)
    return integer_str

def dataframe_to_insert_into_clause(df: pd.DataFrame, table_name: str):
    """
    Converts a Pandas DataFrame to a string INSERT INTO clause.

    Args:
        df: The Pandas DataFrame to convert.
        table_name: The name of the table to insert the data into.

    Returns:
        A string INSERT INTO clause.
    """

    values = []
    for row in df.itertuples():
        values.append("(" + ", ".join(["'" + str(value) + "'" for value in row[1:]]) + ")")

    return "INSERT INTO " + table_name + " (" + ", ".join(df.columns) + ") VALUES " + ", ".join(values) + ";"