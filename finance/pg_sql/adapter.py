from typing import Dict, Any, List

from finance.pg_sql.manager import DBManager


class PostgresSQLAdapter:

    def __init__(self):
        self.manager = DBManager()
        # Do database setup, table creation etc.

    def make_table(self, table_query: str):
        self.manager.execute_query(table_query)

    def multi_get(self, index: str, id_list: list) -> Dict[str, Dict]:
        ...

    def insert(self, table_name: str, data: Dict[Any, Any], **kwargs):
        self.manager.insert(table_name, data)

    def run_query(self, query: Dict[str, Any]):
        return self.manager.execute_query(query)

    def get_by_id(self, table_name, id_value: str) -> List[Dict]:
        return self.manager.get_by_id(table_name, id_value)

    def update(self, table_name: str, object_id, data: Dict, get_updated: bool = False, **kwargs):
        self.manager.update_by_id(table_name, object_id, data, id_key='object_id')
        if get_updated:
            return self.get_by_id(table_name, object_id)

    def delete_by_id(self, table_name: str, id_value: str):
        return self.manager.delete_by_id(table_name, id_value, id_key='object_id')

    def get_all(self, table_name: str, limit: int = 100, offset: int = 0) -> List[Dict]:
        return self.manager.get_all(table_name, limit, offset)

    def get_all_ids(self, table_name: str, field_name: str = 'object_id', limit: int = 100, offset: int = 0):
        return self.manager.get_specific_field(table_name, field_name, limit, offset)





































