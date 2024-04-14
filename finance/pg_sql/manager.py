from typing import List, Dict

import psycopg2
from psycopg2.extensions import cursor
from psycopg2.extras import execute_values
from psycopg2.pool import SimpleConnectionPool

from finance.configs import DBTableNames
from finance.pg_sql.context import PoolCtxManager
from finance.pg_sql.connection_pooling import pool as pg_pool


class DBManager:
    table_names = DBTableNames()

    def __init__(self, pool: SimpleConnectionPool = pg_pool):
        # giving flexibility to provide new pool based on diff config of db hence operating on diff database
        self._connection_pool = pool

    def get_connection_from_pool(self):
        return self._connection_pool.getconn()

    def put_connection_in_pool(self, conn):
        self._connection_pool.putconn(conn)

    @staticmethod
    def get_formatted_output_from_cursor(cursor: cursor) -> List[Dict]:
        if not cursor.description:
            return []
        columns = [i[0] for i in cursor.description]
        rows = cursor.fetchall()
        return [dict(zip(columns, i)) for i in rows]

    def execute_query(self, query: str, values: tuple | list | None = None) -> None | list:
        with PoolCtxManager(self._connection_pool) as cursor:
            cursor.execute(query, values)
            if cursor.description:
                return cursor.fetchall()

    def get_all(self, table_name: str, limit: int = 100, offset: int = 0):
        query = f"SELECT * FROM {table_name} LIMIT {limit} OFFSET {offset}"

        with PoolCtxManager(self._connection_pool) as cursor:
            cursor.execute(query)
            return self.get_formatted_output_from_cursor(cursor)

    def get_by_id(self, table_name: str, id_value: str | int, id_key: str = 'object_id', limit: int = 1, offset: int = 0):
        query = f"SELECT * FROM {table_name} WHERE {id_key} = %s LIMIT {limit} OFFSET {offset}"
        with PoolCtxManager(self._connection_pool) as cursor:
            cursor.execute(query, (id_value,))
            return self.get_formatted_output_from_cursor(cursor)

    def update_by_id(self, table_name: str,  id_value: str,  data: dict, id_key: str = 'id'):
        set_clause = ', '.join([f"{key} = %s" for key in data.keys()])
        query = f"UPDATE {table_name} SET {set_clause} WHERE {id_key} = %s"

        values = list(data.values())
        values.append(id_value)
        with PoolCtxManager(self._connection_pool) as cursor:
            cursor.execute(query, values)

    def insert(self, table_name: str, data: dict):
        columns = ', '.join(data.keys())
        placeholders = ', '.join(['%s' for _ in range(len(data))])

        query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        values = tuple(data.values())

        with PoolCtxManager(self._connection_pool) as cursor:
            cursor.execute(query, values)

    def insert_multi(self, table_name: str, data: list):
        if not data:
            print("No data provided for insertion.")
            return

        columns = data[0].keys()
        query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES %s"

        values = [[row[column] for column in columns] for row in data]

        with PoolCtxManager(self._connection_pool) as cursor:
            execute_values(cursor, query, values)
            print("Data inserted successfully.")

    def get_specific_field(self, table_name: str, field_name: str, limit: int = 100, offset: int = 0):
        query = f"SELECT {field_name} FROM {table_name} LIMIT {limit} OFFSET {offset}"
        with PoolCtxManager(self._connection_pool) as cursor:
            cursor.execute(query)
            return cursor.fetchall()

    def delete_by_id(self, table_name: str, id_value: str, id_key: str = 'id'):
        query = f"DELETE FROM {table_name} WHERE {id_key} = %s"
        with PoolCtxManager(self._connection_pool) as cursor:
            cursor.execute(query, (id_value,))





