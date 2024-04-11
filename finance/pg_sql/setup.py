from psycopg2.errors import DuplicateTable

from finance.pg_sql.manager import DBManager
from finance.configs import DBTableQueries


class DBSetup:
    manager: DBManager = DBManager()

    def __init__(self):
        self.tables = DBTableQueries().to_dict()

    def _setup_table(self):
        try:
            for table_sql_query in self.tables.values():
                self.manager.execute_query(table_sql_query)
        except DuplicateTable as err:
            print(err)

    @classmethod
    def setup(cls):
        obj = cls()
        # do something else
        obj._setup_table()
        return obj










