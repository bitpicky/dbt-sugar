"""
Module base connector.

Only use this class implemented by a child connector.
"""
from abc import ABC
from typing import Any, Dict, List, Optional, Tuple

import sqlalchemy


class BaseConnector(ABC):
    """
    Class base connector.

    Parent class of all the connectors.
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
        self.engine = sqlalchemy.create_engine(**connection_params)

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
        inspector = sqlalchemy.engine.reflection.Inspector.from_engine(self.engine)
        columns = inspector.get_columns(target_table, target_schema)
        columns_names = [column["name"] for column in columns]
        return columns_names

    def run_test(self, test_name: str, schema: str, table: str, column: str) -> bool:
        """
        Method to run pre-defined tests before we add them to the schema.yaml.

        We defined the test dictionary and call the test that the user wants.

        Args:
            test_name(str): Name of the test to run. (For now only "unique" and "not null" are supported).
            schema (str): Name of the schema in which the table to be tested lives.
            table (str): Name of the table to on which to run the test.
            column (str): Name of the column on which to run the test.
        Returns:
            boolean: True if the test is passes, and False if it fails.
        """
        TESTS = {
            "unique": f"""select count(*) as errors from(
                select {column} from {schema}.{table} where {column} is not null group by {column} having count(*) > 1 )
                errors""",
            "not_null": f"select count(*) as errors from {schema}.{table} where {column} is null",
        }
        query = TESTS[test_name]
        result = self.execute_and_check(query)
        return result

    def execute_and_check(self, query) -> bool:
        """
        Method to run a test query and check test results.

        Args:
            query(str): SQL query string to execute.
        Returns:
            boolean: True if the test passes, and False if it fails.
        """
        with self.engine.connect() as cursor:
            result = cursor.execute(query).fetchone()
            if result[0] < 1:
                return True
        return False
