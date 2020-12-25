"""
Module Snowflake connector.

Module dependent of the base connector.
"""
from typing import List

import sqlalchemy
from base import BaseConnector


class SnowflakeConnector(BaseConnector):
    """
    Connection class for Snowflake.

    Child class of base connector.
    """

    def __init__(self, user: str, password: str, database: str, account: str) -> None:
        """
        Init method to instanciatee the credentials.

        Args:
            user (str): user name.
            password (str): password.
            database (str): database name.
            account (str): account name.
        """
        self.connection_details = f"snowflake://{user}:{password}@{account}/{database}"

    def generate_connection(self) -> sqlalchemy.engine:
        """
        Method that creates the connection.

        Returns:
            sqlalchemy.engine: Engine to connect to the database.
        """
        return sqlalchemy.create_engine(self.connection_details)

    def get_columns_from_table(self, table: str, database: str) -> List[str]:
        """
        Method to get the columns from a table.

        Args:
            table (str): table name.
            database (str): database where the table is.

        Returns:
            Optiona[List[str]]: with the list of columns in the table.
        """
        rows = self.run_query(
            connection=self.generate_connection(),
            query=f" SELECT * FROM {database}.information_schema.columns WHERE table_name = '{table.upper()}'",
        )
        columns_names = [row[3] for row in rows]
        return columns_names
