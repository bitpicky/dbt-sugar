"""
Module Snowflake connector.

Module dependent of the base connector.
"""
from typing import Dict

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
        connection_params: Dict[str, str],
    ) -> None:
        """
        Init method to instanciate the credentials.

        Args:
            connection_params (Dict[str, str]): Dict with connection parameters.
        """
        self.connection_url = URL(
            drivername="postgresql+psycopg2",
            user=connection_params.get("user", str()),
            password=connection_params.get("password", str()),
            database=connection_params.get("database", str()),
            account=connection_params.get("account", str()),
        )
        self.engine = sqlalchemy.create_engine(self.connection_url)
