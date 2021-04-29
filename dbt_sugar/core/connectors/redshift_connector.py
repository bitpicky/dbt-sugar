"""
Redshift connector module.

Extends BaseConnector.
"""
from typing import Dict

import sqlalchemy

from dbt_sugar.core.connectors.base import BaseConnector


class RedshiftConnector(BaseConnector):
    """
    Connection class for Redshift databases.

    Extends BaseConnector.
    """

    def __init__(
        self,
        connection_params: Dict[str, str],
    ) -> None:
        """
        Creates the URL and the Engine for future connections.

        Args:
            connection_params (Dict[str, str]): Dict containing database connection
                parameters and credentials.
        """
        self.connection_url = sqlalchemy.engine.url.URL(
            drivername="redshift+psycopg2",
            host=connection_params.get("host", str()),
            username=connection_params.get("user", str()),
            password=connection_params.get("password", str()),
            database=connection_params.get("database", str()),
            port=connection_params.get("port", str()),
        )
        self.engine = sqlalchemy.create_engine(self.connection_url)
