import pymssql
from contextlib import contextmanager
import os

server      = os.getenv('LUMINATE_SERVER')
user        = os.getenv('LUMINATE_USER')
password    = os.getenv('LUMINATE_PASSWORD')
database    = os.getenv('LUMINATE_DATABASE')
port        = os.getenv('LUMINATE_PORT')

@contextmanager
def connection_to_sql_server():
    """
    Establish sql server connection
    """
    connection = pymssql.connect(
        server=     server,
        user=       user,
        password=   password,
        database=   database,
        port =      port
    )

    try:
        yield connection

    except:
        connection.close()