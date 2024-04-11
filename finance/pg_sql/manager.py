import psycopg2
from psycopg2.extras import execute_values
from psycopg2.pool import SimpleConnectionPool

from finance.pg_sql.context import ConnectionCtxManager
from finance.pg_sql.connection_pooling import pool as pg_pool
from finance.configs import DBTableNames


class DBManager:
    table_names = DBTableNames()

    def __init__(self, pool: SimpleConnectionPool = pg_pool):
        # giving flexibility to provide new pool based on diff config of db hence operating on diff database
        self._connection_pool = pool

    def get_connection_from_pool(self):
        return self._connection_pool.getconn()

    def put_connection_in_pool(self, conn):
        self._connection_pool.putconn(conn)

    def execute_query(self, query: str):
        with ConnectionCtxManager(self._connection_pool) as cursor:
            cursor.execute(query)

    def get(self, table_name: str, limit: int = 100, offset: int = 0):
        query = f"SELECT * FROM {table_name} LIMIT {limit} OFFSET {offset}"

        with ConnectionCtxManager(self._connection_pool) as cursor:
            cursor.execute(query)
            return cursor.fetchall()

    def update(self, table_name: str, data: dict, condition: str):
        set_clause = ', '.join([f"{key} = %s" for key in data.keys()])

        query = f"UPDATE {table_name} SET {set_clause}"
        if condition:
            query += f" WHERE {condition}"  # sql injection?

        with ConnectionCtxManager(self._connection_pool) as cursor:
            cursor.execute(query, list(data.values()))

    def insert(self, table_name: str, data: dict):
        columns = ', '.join(data.keys())
        placeholders = ', '.join(['%s' for _ in range(len(data))])

        query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        values = tuple(data.values())

        with ConnectionCtxManager(self._connection_pool) as cursor:
            cursor.execute(query, values)

    def insert_multi(self, table_name: str, data: list):
        if not data:
            print("No data provided for insertion.")
            return

        columns = data[0].keys()
        query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES %s"

        values = [[row[column] for column in columns] for row in data]

        with ConnectionCtxManager(self._connection_pool) as cursor:
            execute_values(cursor, query, values)
            print("Data inserted successfully.")



