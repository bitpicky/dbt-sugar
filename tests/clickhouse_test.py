from unittest.mock import call

import pytest
import sqlalchemy

from dbt_sugar.core.connectors.clickhouse_connector import ClickhouseConnector

CREDENTIALS = {
    "username": "dbt_sugar_test_user",
    "password": "magical_password",
    "database": "dbt_sugar",
    "host": "some_host",
    "port": 9000,
}


def test_generate_connection():
    conn = ClickhouseConnector(CREDENTIALS)
    assert isinstance(conn.engine, sqlalchemy.engine.Engine)


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
def test_execute_and_check(mocker, test_name, schema, table, column_name, result):
    execute_and_check = mocker.patch(
        "dbt_sugar.core.connectors.clickhouse_connector.ClickhouseConnector.execute_and_check"
    )
    clickhouse_connector = ClickhouseConnector(CREDENTIALS)
    clickhouse_connector.run_test(test_name, schema, table, column_name)
    execute_and_check.assert_has_calls(result)
