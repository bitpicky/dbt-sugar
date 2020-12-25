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

    def run_query(
        self,
        connection: sqlalchemy.engine,
        query: str,
    ) -> Optional[List[Tuple[Any]]]:
        """
        Method that creates cursor to run a query.

        Args:
            connection (sqlalchemy.engine): Engine connection.
            query (str): sql query.

        Returns:
            Optional[List[Tuple[Any]]]: With the results of the query.
        """
        with connection.connect() as con:
            rows = con.execute(query)
            return rows

    @abstractmethod
    def get_columns_from_table(self, table: str) -> Optional[List[str]]:
        """
        Method to get the columns names from a table.

        Args:
            table (str): table name.

        Returns:
            Optiona[List[str]]: with the list of columns in the table.
        """
        logging.info(f"Getting the columns from table {table}")
