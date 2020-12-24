from pathlib import Path


TEST_DIR = Path(__file__).resolve().parent


def test_select(postgresql_db):
    import pandas

    # this populates the db with stuff from the sql file.
    # We could also just run a conn.execute if we wanted to.
    postgresql_db.run_sql_file(str(TEST_DIR.joinpath("db_setup.sql")))
    with postgresql_db.engine.connect() as conn:
        r = conn.execute("SELECT * FROM test")
        results = list()
        for row in r:
            results.append(dict(row))
        print(results)
        print(pandas.DataFrame.from_records(results))


c = create_engine("postgresql://root:password@localhost/dbt_sugar")
