"""
Module Snowflake connector.

Module dependent of the base connector.
"""
from typing import Optional

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
        host: Optional[str] = None,
    ) -> None:
        """
        Init method to instanciatee the credentials.

        Args:
            user (str): user name.
            password (str): password.
            account (str): account name.
            database (str): database name.
            host(Optional[str]): host name.
        """
        self.connection_url = URL(
            account=account,
            user=user,
            password=password,
            database=database,
        )
        self.engine = sqlalchemy.create_engine(self.connection_url)
