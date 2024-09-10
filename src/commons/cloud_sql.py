from google.cloud.sql.connector import Connector
from contextlib import contextmanager
import sqlalchemy
import os
import pandas as pd
from utils.strings import dataframe_to_insert_into_clause
from utils.logging_decorator import exception, logger

instance    = os.getenv('CLOUD_SQL_INSTANCE')
user        = os.getenv('CLOUD_SQL_USER')
password    = os.getenv('CLOUD_SQL_PASSWORD')
database    = os.getenv('CLOUD_SQL_DATABASE')

@exception(logger)
class CloudSQL:
    """
    A class for interacting with a PostgreSQL database hosted on Cloud SQL.
    """

    @contextmanager
    def get_postgres_engine(self):
        """
        Creates a context manager that yields a SQLAlchemy engine object for interacting with the PostgreSQL database.

        Yields:
            sqlalchemy.engine.Engine: The SQLAlchemy engine object.
        """
        def get_conn():
            """
            Establishes a connection to the PostgreSQL database.

            Returns:
                pg8000.connection: The PostgreSQL connection object.
            """
            connection = Connector().connect(
                instance,
                "pg8000",
                user=user,
                password=password,
                db=database
            )
            return connection

        try:
            engine = sqlalchemy.create_engine(
                "postgresql+pg8000://",
                creator=get_conn,
            )
            yield engine

        finally:
            engine.dispose()

    def retrieve(self, query: str) -> pd.DataFrame:
        """
        Retrieves data from the PostgreSQL database and returns it as a Pandas DataFrame.

        Args:
            query: The SQL query to execute.
        """
        with self.get_postgres_engine() as engine:
            connection = engine.connect()
            results = connection.execute(sqlalchemy.text(query))
            df = pd.DataFrame(results.fetchall())
        return df
    
    def insert_data(self, df: pd.DataFrame, table_name: str) -> None:
        """
        Inserts data from a Pandas DataFrame into the PostgreSQL database.

        Args:
            df: The Pandas DataFrame containing the data to insert.
            table_name: The name of the table to insert the data into.
        """
        with self.get_postgres_engine() as engine:
            connection = engine.raw_connection()
            cursor = connection.cursor()
            insert_clause = dataframe_to_insert_into_clause(df, table_name)
            cursor.execute(insert_clause)
            connection.commit()
        