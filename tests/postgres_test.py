from unittest.mock import call

import pytest
import sqlalchemy

from dbt_sugar.core.connectors.postgres_connector import PostgresConnector

CREDENTIALS = {
    "username": "dbt_sugar_test_user",
    "password": "magical_password",
    "database": "dbt_sugar",
    "host": "localhost",
    "port": "5432",
}


def test_generate_connection():
    from dbt_sugar.core.connectors.postgres_connector import PostgresConnector

    conn = PostgresConnector(CREDENTIALS)
    assert isinstance(conn.engine, sqlalchemy.engine.Engine)


def test_get_columns_from_table():
    from dbt_sugar.core.connectors.postgres_connector import PostgresConnector

    expectation = ["id", "answer", "question"]

    columns = PostgresConnector(CREDENTIALS).get_columns_from_table(
        target_schema="public", target_table="test"
    )
    assert columns == expectation


@pytest.mark.parametrize(
    "test_name, schema, table, column_name, result",
    [
        (
            "unique",
            "schema",
            "table",
            "column",
            [
                call(
                    """select count(*) as errors from(
                select column from schema.table where column is not null group by column having count(*) > 1 )
                errors"""
                )
            ],
        ),
        (
            "not_null",
            "schema",
            "table",
            "column",
            [call("select count(*) as errors from schema.table where column is null")],
        ),
    ],
)
def test_run_test(mocker, test_name, schema, table, column_name, result):
    execute_and_check = mocker.patch(
        "dbt_sugar.core.connectors.postgres_connector.PostgresConnector.execute_and_check"
    )
    postgres_connector = PostgresConnector(CREDENTIALS)
    postgres_connector.run_test(test_name, schema, table, column_name)
    execute_and_check.assert_has_calls(result)
