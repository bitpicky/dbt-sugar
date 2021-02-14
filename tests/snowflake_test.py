from unittest.mock import call

import pytest
import sqlalchemy

from dbt_sugar.core.connectors.snowflake_connector import SnowflakeConnector

CREDENTIALS = dict(
    user="dbt_sugar_test_user",
    password="magical_password",
    database="dbt_sugar",
    account="dummy_account",
)


def test_generate_connection():
    conn = SnowflakeConnector(**CREDENTIALS)
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
def test_run_test(mocker, test_name, schema, table, column_name, result):
    run_query_test = mocker.patch(
        "dbt_sugar.core.connectors.snowflake_connector.SnowflakeConnector.run_query_test"
    )
    snowflake_connector = SnowflakeConnector(**CREDENTIALS)
    snowflake_connector.run_test(test_name, schema, table, column_name)
    run_query_test.assert_has_calls(result)
