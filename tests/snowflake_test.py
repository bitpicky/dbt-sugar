import sqlalchemy

CREDENTIALS = dict(
    user="dbt_sugar_test_user",
    password="magical_password",
    database="dbt_sugar",
    account="dummy_account",
)


def test_generate_connection():
    from dbt_sugar.core.connectors.snowflake_connector import SnowflakeConnector

    conn = SnowflakeConnector(**CREDENTIALS)
    assert isinstance(conn.engine, sqlalchemy.engine.Engine)
