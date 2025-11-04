from airflow.sdk import Connection
from airflow.models import Variable
from psycopg2.pool import SimpleConnectionPool
import logging


class SQLPool:
    def __init__(self, **kwargs):
        self.conn = self.connection()

    def connection(self):
        return Connection.get('postgres_folio')

    def pool(self, **kwargs):
        conn = self.conn
        max_pool_size = Variable.get("max_pool_size", 48)
        logging.getLogger(__name__).info(f"SQL max pool size: {max_pool_size}")
        return SimpleConnectionPool(
            12,
            max_pool_size,
            database='okapi',
            host=conn.host,
            password=conn.password,
            port=conn.port,
            user='okapi',
        )
