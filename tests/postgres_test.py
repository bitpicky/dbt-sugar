import sqlalchemy


def generate_connection():
    from dbt_sugar.core.connectors.postgres_connector import PostgresConnector

    credentials = dict(
        user="dbt_sugar_test_user", password="magical_password", database="dbt_sugar"
    )
    conn = PostgresConnector(**credentials).generate_connection()

    # assert isinstance(conn.engine, sqlalchemy.engine.Engine)
