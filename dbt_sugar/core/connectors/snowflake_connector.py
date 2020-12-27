"""
Module Snowflake connector.

Module dependent of the base connector.
"""
from typing import Any, List, Optional, Tuple

import sqlalchemy
from snowflake.sqlalchemy import URL

from dbt_sugar.core.connectors.base import BaseConnector


class SnowflakeConnector(BaseConnector):
    """
    Connection class for Snowflake.

    Child class of base connector.
    """

    def __init__(
        self,
        user: str,
        password: str,
        account: str,
        database: str,
    ) -> None:
        """
        Init method to instanciatee the credentials.

        Args:
            user (str): user name.
            password (str): password.
            account (str): account name.
            database (str): database name.
        """
        self.connection_url = URL(
            account=account,
            user=user,
            password=password,
            database=database,
        )

    def generate_connection(self) -> sqlalchemy.engine:
        """
        Method that creates the connection.

        Returns:
            sqlalchemy.engine: Engine to connect to the database.
        """
        return sqlalchemy.create_engine(self.connection_url)

    def get_columns_from_table(
        self,
        target_table: str,
        target_schema: str,
    ) -> Optional[List[Tuple[Any]]]:
        """
        Method that creates cursor to run a query.

        Args:
            target_table (str): table to get the columns from.
            target_schema (str): schema to get the table from.

        Returns:
            Optional[List[Tuple[Any]]]: With the results of the query.
        """
        engine = self.generate_connection()
        return super().get_columns_from_table(engine, target_table, target_schema)
