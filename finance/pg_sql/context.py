import psycopg2
from psycopg2 import pool as psycopg_connection_pool


class ConnectionCtxManager:
    def __init__(self, pool: psycopg_connection_pool):
        self.pool: pool = pool
        self.connection: psycopg2.connection = self.pool.getconn()

    def __enter__(self):
        self.cursor = self.connection.cursor()
        return self.cursor

    def __exit__(self, exc_type, exc_value, traceback):
        try:
            print('committing changes and closing connection')
            self.connection.commit()
        except Exception as ex:
            self.connection.rollback()
            print(f"Error occurred during commit: {ex}")
            raise ex
        finally:
            self.cursor.close()
            self.pool.putconn(self.connection)
            print('wrapped up!')
