"""
Module base connector.

Only use this class implemented by a child connector.
"""
from typing import Any, List, Optional, Tuple

import sqlalchemy
from snowflake.sqlalchemy import URL

from dbt_sugar.core.logger import GLOBAL_LOGGER as logger


class BaseConnector:
    """
    Class base connector.

    Parent class of all the connectors.
    """

    def __init__(
        self,
        user: str,
        password: str,
        database: str,
        connector_type: Optional[str] = None,
        host: Optional[str] = None,
        account: Optional[str] = None,
    ):
        """
        Init method to instanciatee the credentials.

        Args:
            user (str): user name.
            password (str): password.
            database (str): database name.
            host (str): host name.
            account (str): account name.
        """
        if connector_type == "snowflake":
            self.connection_url = URL(
                account=account,
                user=user,
                password=password,
                database=database,
            )
        elif connector_type == "postgres":
            self.connection_url = sqlalchemy.engine.url.URL(
                drivername="postgresql+psycopg2",
                host=host,
                username=user,
                password=password,
                database=database,
            )
        else:
            logger.error(f"The type of connector {connector_type} is not available in dbt_sugar.")

    def generate_connection(self) -> sqlalchemy.engine:
        """
        Method that creates the connection.

        Returns:
            sqlalchemy.engine: Engine to connect to the database.
        """
        return sqlalchemy.create_engine(self.connection_url)

    def get_columns_from_table(
        self,
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
        connection = self.generate_connection()
        inspector = sqlalchemy.engine.reflection.Inspector.from_engine(connection)
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

    def run_test_unique(self, schema: str, table: str, column: str) -> bool:
        """
        Method to check that a column have unique values.

        Args:
            schema (str): schema to run the test from.
            table (str): table to run the test from.
            column (str): column name to test.

        Returns:
            boolean: True if the test is okay, and False if the test doesn't pass.
        """
        engine = self.generate_connection()
        query = f"""
                select count(*) as errors from
                (
                    select {column} from {schema}.{table}
                    where {column} is not null
                    group by {column}
                    having count(*) > 1
                ) errors
            """
        with engine.connect() as con:
            result = con.execute(query)
            for row in result:
                if row[0] > 0:
                    return False
                return True

    def run_test_not_null(self, schema: str, table: str, column: str) -> bool:
        """
        Method to check that a column doesn't have any null values.

        Args:
            schema (str): schema to run the test from.
            table (str): table to run the test from.
            column (str): column name to test.

        Returns:
            boolean: True if the test is okay, and False if the test doesn't pass.
        """
        engine = self.generate_connection()
        query = f"""
                    select count(*) as errors
                    from {schema}.{table}
                    where {column} is null
                """
        with engine.connect() as con:
            result = con.execute(query)
            for row in result:
                if row[0] > 0:
                    return False
                return True
