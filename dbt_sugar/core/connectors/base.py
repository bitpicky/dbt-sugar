"""
Module base connector.

Only use this class implemented by a child connector.
"""
import logging
from abc import ABC, abstractmethod
from typing import Any, List, Optional, Tuple

import sqlalchemy


class BaseConnector(ABC):
    """
    Class base connector.

    Parent class of all the connectors.
    """

    @abstractmethod
    def generate_connection(self) -> sqlalchemy.engine:
        """
        Method that creates the connection.

        Returns:
            sqlalchemy.engine: Engine to connect to the database.
        """
        logging.info("Genetating connection")

    def get_columns_from_table(
        self,
        connection: sqlalchemy.engine,
        target_table: str,
        target_schema: str,
    ) -> Optional[List[Tuple[Any]]]:
        """
        Method that creates cursor to run a query.

        Args:
            connection (sqlalchemy.engine): engine to make the connection to the db.
            target_table (str): table to get the columns from.
            target_schema (str): schema to get the table from.

        Returns:
            Optional[List[Tuple[Any]]]: With the results of the query.
        """
        inspector = sqlalchemy.engine.reflection.Inspector.from_engine(connection)
        columns = inspector.get_columns(target_table, target_schema)
        columns_names = [column["name"] for column in columns]
        return columns_names
