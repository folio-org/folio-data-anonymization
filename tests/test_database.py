import pytest
import psycopg
import os
from pytest_postgresql import factories
from plugins.database import Database
from plugins.users import Users


@pytest.fixture
def env_vars() -> dict:
    return {
        "PGHOST": os.environ["PGHOST"],
        "PGPORT": os.environ["PGPORT"],
        "PGUSER": os.environ["PGUSER"],
        "PGPASSWORD": os.environ["PGPASSWORD"],
        "PGDATABASE": os.environ["PGDATABASE"],
        "TENANT": os.environ["TENANT"],
    }


def load_database(**kwargs):
    db_connection = psycopg.connect(**kwargs)
    with db_connection.cursor() as cur:
        cur.execute("CREATE TABLE users (id serial PRIMARY KEY, name varchar);")
        cur.execute(
            "INSERT INTO users (name) VALUES"
            "('John Doe'), ('Jane Doe'), ('Mary Smith');"
        )
        db_connection.commit()


postgresql_proc = factories.postgresql_proc(
    host=os.getenv("PGHOST"),
    port=os.getenv("PGPORT"),
    user=os.getenv("PGUSER"),
    password=os.getenv("PGPASSWORD"),
    dbname=os.getenv("PGDATABASE"),
    load=[load_database],
)

postgresql = factories.postgresql(process_fixture_name="postgresql_proc")


def test_load_env_vars_pytest_env(env_vars):
    assert env_vars["PGHOST"] == "localhost"
    assert int(env_vars["PGPORT"]) == 5432
    assert env_vars["PGUSER"] == "test_user"
    assert env_vars["PGPASSWORD"] == "admin123"
    assert env_vars["PGDATABASE"] == "dbname"
    assert env_vars["TENANT"] == "diku"


def test_query_db(postgresql):
    cur = postgresql.execute("SELECT * FROM users WHERE id = %s;", ["1"], binary=True)
    (id, data) = cur.fetchone()
    assert data == 'John Doe'


def test_database_instance(env_vars, postgresql):
    db = Database()
    db_connection = db.connection
    assert db_connection.info.host == env_vars["PGHOST"]
    assert db_connection.info.port == int(env_vars["PGPORT"])
    assert db_connection.info.user == env_vars["PGUSER"]
    assert db_connection.info.password == env_vars["PGPASSWORD"]
    assert db_connection.info.dbname == env_vars["PGDATABASE"]


def test_schema_name(env_vars, postgresql):
    db = Database()
    name = db.schema_name('mod_users')
    assert name == 'diku_mod_users'


def test_table_name(env_vars, postgresql):
    db = Database()
    name = db.table_name('mod_users', 'users')
    assert name == 'diku_mod_users.users'


def test_anonymize_users(mocker, postgresql, caplog):
    mocker.patch(
        "plugins.users.Users", return_value=postgresql
    )
    users_task = Users()
    users_task.anonymize_users()
    assert "Anonymizing diku_mod_users.users" in caplog.text
