"""
Module Snowflake connector.

Module dependent of the base connector.
"""
from typing import List

import snowflake.connector
from base import BaseConnector


class SnowflakeConnector(BaseConnector):
    """
    Connection class for Snowflake.

    Child class of base connector.
    """

    def __init__(self, user: str, password: str, database: str, account: str) -> None:
        """
        Init method to instanciatee the credentials.

        :param user: string with the user name.
        :param password: string with the password.
        :param database: string with the database name.
        :param account: string with the account name.
        """
        self.connection_details = dict(
            account=account,
            user=user,
            password=password,
            database=database,
        )

    def generate_connection(self) -> snowflake.connector.connection:
        """
        Method that creates the connection.

        :return: snowflake.connector.connection
        """
        return snowflake.connector.connect(**self.connection_details)

    def get_columns_from_table(self, database: str, table: str) -> List[str]:
        """
        Method to get the columns from a table.

        :param table: string with the table name.
        :return: Optiona[List[str]]
        """
        rows = self.run_query(
            connection=self.generate_connection(),
            query=f" SELECT * FROM {database}.information_schema.columns WHERE table_name = '{table.upper()}'",
        )
        columns_names = [row[3] for row in rows]
        return columns_names
