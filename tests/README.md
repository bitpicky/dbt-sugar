# Testing our DB connectors

## Using Postgres from a Docker :whale: container

### Spin Up the DB

In the `/docker_postgres` folder you can find the following files:

- `docker-compose.yml`
- `db_setup.sql`

To launch the db you can call `docker-compose up` from the `/docker_postgres` folder and this should take care of everything. This means the docker will be created and the db will be populated from the `db_setup.sql` file. We can add as many as we want.

### Connect to the DB from Python

Once the db is up and the container is running it's as easy as creating a connection with `sqlachemy`

```python
from sqlalchemy import create_engine

con = c.create_engine("postgresql://root:password@localhost/dbt_sugar")
result = con.execute("SELECT * FROM test;")

# to see the results we can construct a list of dicts easily --or whatever we need.
query_results = list()

for row in result:
    query_results.append(dict(row))
```

This should return something like this

```python
[{'id': 1, 'answer': 11, 'question': 'hello hello world'},
 {'id': 2, 'answer': 42, 'question': 'what is the meaning of life?'}]
```

## Testing in GH Actions

We can also spring up a container in the github action that will run the testing.

More info on this later when I have time to set it up.

## Using `pytest` fixtures :sparkles:

Alternatively, as can be seen in `db_test.py` we can use the `pytest-pgsql` library to fake a connection and run the tests from there. This is handy as it removes having to construct the docker but it means we may not be able to test absolutely **every** bit of our connectors (namely the engine creation and connection) so we will have to see what our needs are in the future.
