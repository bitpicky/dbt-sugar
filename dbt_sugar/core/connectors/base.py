"""
Module base connector.

Only use this class implemented by a child connector.
"""
import logging
from typing import Any, List, Optional, Tuple


class BaseConnector:
    """
    Class base connector.

    Parent class of all the connectors.
    """

    def generate_connection(self) -> None:
        """
        Method that creates the connection.

        :return: the specific connection of each Child.
        """
        logging.info("Genetating connection")

    def run_query(
        self,
        connection: Any,
        query: str,
    ) -> Optional[List[Tuple[Any]]]:
        """
        Method that creates cursor to run a query.

        :param query: string with the query to run.
        :return: Optiona[List[str]]
        """
        try:
            cursor_connection = connection.cursor()
            cursor_connection.execute(query)
            rows = cursor_connection.fetchall()
            return rows
        except Exception as e:
            logging.error(f"Not able to get the data from the database, with error: {e}.")
            return None
        finally:
            cursor_connection.close()
            connection.close()

    def get_columns_from_table(self, table: str) -> List[str]:
        """
        Method to get the columns names from a table.

        :param table: string with the table name.
        :return: List[str]
        """
        logging.info(f"Getting the columns from table {table}")
