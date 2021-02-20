from unittest.mock import call

import pytest
import sqlalchemy

from dbt_sugar.core.connectors.snowflake_connector import SnowflakeConnector

CREDENTIALS = {
    "user": "dbt_sugar_test_user",
    "password": "magical_password",
    "database": "dbt_sugar",
    "account": "dummy_account",
}


def test_generate_connection():
    conn = SnowflakeConnector(CREDENTIALS)
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
        "dbt_sugar.core.connectors.snowflake_connector.SnowflakeConnector.execute_and_check"
    )
    snowflake_connector = SnowflakeConnector(CREDENTIALS)
    snowflake_connector.run_test(test_name, schema, table, column_name)
    execute_and_check.assert_has_calls(result)
