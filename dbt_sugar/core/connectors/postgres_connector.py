"""
Module Postgres connector.

Module dependent of the base connector.
"""
from typing import Optional

import sqlalchemy

from dbt_sugar.core.connectors.base import BaseConnector


class PostgresConnector(BaseConnector):
    """
    Connection class for Postgres.

    Child class of base connector.
    """

    def __init__(
        self,
        user: str,
        password: str,
        database: str,
        host: str = "localhost",
        account: Optional[str] = None,
    ) -> None:
        """
        Init method to instanciate the credentials.

        Args:
            user (str): user name.
            password (str): password.
            database (str): database name.
            host (str): host name.
            account(Optional[str]): account name.
        """
        self.connection_url = sqlalchemy.engine.url.URL(
            drivername="postgresql+psycopg2",
            host=host,
            username=user,
            password=password,
            database=database,
        )
        self.engine = sqlalchemy.create_engine(self.connection_url)
