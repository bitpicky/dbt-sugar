"""
Module Postgres connector.

Module dependent of the base connector.
"""
from typing import List

import sqlalchemy
from base import BaseConnector


class PostgresConnector(BaseConnector):
    """
    Connection class for Postgres.

    Child class of base connector.
    """

    def __init__(self, user: str, password: str, database: str, host: str = "localhost") -> None:
        """
        Init method to instanciatee the credentials.

        Args:
            user (str): user name.
            password (str): password.
            database (str): database name.
            host (str): host name.
        """
        self.connection_string = f"postgresql://{user}:{password}@{host}/{database}"

    def generate_connection(self) -> sqlalchemy.engine:
        """
        Method that creates the connection.

        Returns:
            sqlalchemy.engine: Engine to connect to the database.
        """
        return sqlalchemy.create_engine(self.connection_string)

    def get_columns_from_table(self, table: str) -> List[str]:
        """
        Method to get the columns from a table.

        Args:
            table (str): table name.

        Returns:
            Optiona[List[str]]: with the list of columns in the table.
        """
        rows = self.run_query(
            connection=self.generate_connection(),
            query=f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table}';",
        )
        columns_names = [row[0] for row in rows]
        return columns_names
