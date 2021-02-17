"""
Module Postgres connector.

Module dependent of the base connector.
"""
from typing import Dict

import sqlalchemy

from dbt_sugar.core.connectors.base import BaseConnector


class PostgresConnector(BaseConnector):
    """
    Connection class for Postgres.

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
        self.connection_url = sqlalchemy.engine.url.URL(
            drivername="postgresql+psycopg2",
            **connection_params,
        )
        self.engine = sqlalchemy.create_engine(self.connection_url)
