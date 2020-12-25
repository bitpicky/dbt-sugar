from pathlib import Path


TEST_DIR = Path(__file__).resolve().parent

EXPECTATION = [
    {"id": 1, "answer": 11, "question": "hello hello world"},
    {"id": 2, "answer": 42, "question": "what is the meaning of life?"},
]


# this test uses the pytest fixture to simulate a db just for the test
def test_select(postgresql_db):
    # this populates the db with stuff from the sql file.
    # We could also just run a conn.execute if we wanted to.
    postgresql_db.run_sql_file(str(TEST_DIR.joinpath("docker_postgres", "db_setup.sql")))
    with postgresql_db.engine.connect() as conn:
        r = conn.execute("SELECT * FROM test")
        results = list()
        for row in r:
            results.append(dict(row))
        print(results)

    assert results == EXPECTATION


# this test actually connects to a db that is reachable from a docker container that is spun up
#  before the test suite is called
def test_select_from_real_db():
    from sqlalchemy import create_engine

    con = create_engine("postgresql://root:password@localhost/dbt_sugar")
    result = con.execute("SELECT * FROM test;")

    # to see the results we can construct a list of dicts easily --or whatever we need.
    query_results = list()

    for row in result:
        query_results.append(dict(row))

    print(query_results)
    assert query_results == EXPECTATION
