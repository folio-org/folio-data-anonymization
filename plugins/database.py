import psycopg
import os


class Database(object):
    def __init__(self):
        self.connection = psycopg.connect(
            host=os.getenv("PGHOST"),
            port=os.getenv("PGPORT"),
            user=os.getenv("PGUSER"),
            password=os.getenv("PGPASSWORD"),
            dbname=os.getenv("PGDATABASE"),
        )

    def schema_name(self, schema: str) -> str:
        return f"{os.getenv('TENANT')}_{schema}"

    def table_name(self, schema: str, table: str) -> str:
        return f"{self.schema_name(schema)}.{table}"
