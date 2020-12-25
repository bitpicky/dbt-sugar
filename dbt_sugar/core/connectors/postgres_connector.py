"""
Module Postgres connector.

Module dependent of the base connector.
"""
from typing import List

import psycopg2
from base import BaseConnector


class PostgresConnector(BaseConnector):
    """
    Connection class for Postgres.

    Child class of base connector.
    """

    def __init__(self, user: str, password: str, database: str, host: str = "localhost") -> None:
        """
        Init method to instanciatee the credentials.

        :param user: string with the user name.
        :param password: string with the password.
        :param database: string with the database name.
        :param host: string with the host name.
        """
        self.connection_details = dict(
            host=host,
            user=user,
            password=password,
            database=database,
        )

    def generate_connection(self):
        """
        Method that creates the connection.

        :return: psycopg2.connector.connection
        """
        return psycopg2.connect(**self.connection_details)

    def get_columns_from_table(self, table: str) -> List[str]:
        """
        Method to get the columns from a table.

        :param table: string with the table name.
        :return: Optiona[List[str]]
        """
        rows = self.run_query(
            connection=self.generate_connection(),
            query=f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table}';",
        )
        columns_names = [row[0] for row in rows]
        return columns_names
