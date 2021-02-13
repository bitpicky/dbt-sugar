"""
Module base connector.

Only use this class implemented by a child connector.
"""
from abc import ABC
from typing import Any, List, Optional, Tuple

import sqlalchemy

from dbt_sugar.core.logger import GLOBAL_LOGGER as logger


class BaseConnector(ABC):
    """
    Class base connector.

    Parent class of all the connectors.
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
        Method to run the all the tests.

        We defined the test dictionary and call the test that the user wants.

        Args:
            test_name(str): name of the test.
            schema (str): schema to run the test from.
            table (str): table to run the test from.
            column (str): column name to test.
        Returns:
            boolean: True if the test is okay, and False if the test doesn't pass.
        """
        TESTS = {
            "unique": self.run_test_unique,
            "not_null": self.run_test_not_null,
        }
        method = TESTS.get(test_name)
        result = method(schema, table, column)
        if result:
            logger.info(f"The {test_name} test in the column: {column}, have run successfully.")
        else:
            logger.info(f"The {test_name} test in the column: {column}, have not run successfully.")
        return result

    def run_test_unique(self, schema: str, table: str, column_name: str) -> bool:
        """
        Method to check that a column have unique values.

        Args:
            schema (str): schema to run the test from.
            table (str): table to run the test from.
            column (str): column name to test.
        Returns:
            boolean: True if the test is okay, and False if the test doesn't pass.
        """
        query = f"""
                select count(*) as errors from
                (
                    select {column_name} from {schema}.{table}
                    where {column_name} is not null
                    group by {column_name}
                    having count(*) > 1
                ) errors
        """
        with self.engine.connect() as cursor:
            result = cursor.execute(query).fetchone()
            if result[0] < 1:
                return True
        return False

    def run_test_not_null(self, schema: str, table: str, column_name: str) -> bool:
        """
        Method to check that a column doesn't have any null values.

        Args:
            schema (str): schema to run the test from.
            table (str): table to run the test from.
            column_name (str): column name to test.
        Returns:
            boolean: True if the test is okay, and False if the test doesn't pass.
        """
        query = f"""
                    select count(*) as errors
                    from {schema}.{table}
                    where {column_name} is null
        """
        with self.engine.connect() as cursor:
            result = cursor.execute(query).fetchone()
            if result[0] < 1:
                return True
        return False
