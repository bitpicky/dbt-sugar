import sqlalchemy


CREDENTIALS = dict(user="dbt_sugar_test_user", password="magical_password", database="dbt_sugar")


def test_generate_connection():
    from dbt_sugar.core.connectors.postgres_connector import PostgresConnector

    conn = PostgresConnector(**CREDENTIALS).generate_connection()
    assert isinstance(conn.engine, sqlalchemy.engine.Engine)


def test_get_columns_from_table():
    from dbt_sugar.core.connectors.postgres_connector import PostgresConnector

    expectation = ["id", "answer", "question"]

    columns = PostgresConnector(**CREDENTIALS).get_columns_from_table(
        target_schema="public", target_table="test"
    )
    assert columns == expectation
