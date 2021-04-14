"""
Module Snowflake connector.

Module dependent of the base connector.
"""
from typing import Any, Dict, List, Optional, Tuple

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
        Creates the URL and the Engine for future connections.

        Args:
            connection_params (Dict[str, str]): Dict containing database connection
                parameters and credentials.
        """
        self.connection_url = URL(
            drivername="postgresql+psycopg2",
            user=connection_params.get("user", str()),
            password=connection_params.get("password", str()),
            database=connection_params.get("database", str()),
            account=connection_params.get("account", str()),
            warehouse=connection_params.get("warehouse", str()),
        )
        self.engine = sqlalchemy.create_engine(self.connection_url)

    def get_columns_from_table(
        self, target_table: str, target_schema: str, use_describe: bool = False
    ) -> Optional[List[Tuple[Any]]]:

        # if user wants to use describe (more preformant but with caveat) method
        # we re-implement column describe since snowflake.sqlalchemy is shit.
        if use_describe:
            # do some basic escaping to prevent "little bobby drop table" scenario
            target_schema = target_schema.split(";")[0]
            target_table = target_table.split(";")[0]

            connection = self.engine.connect()
            results = connection.execute(f"describe table {target_schema}.{target_table};")
            results = results.fetchall()
            columns = [row["name"] for row in results]
            return columns

        # else we just return the base method which will user snowflake.sqlalchemy's impl
        return super().get_columns_from_table(target_table, target_schema)
