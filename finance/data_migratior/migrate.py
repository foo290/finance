from finance.pg_sql.manager import DBManager
from finance.data_migratior.export_payment_data import process_payment_data


manager = DBManager()


def migrate_data(payment_data_file_name: str):
    payment_data = process_payment_data(payment_data_file_name)
    manager.insert_multi(manager.table_names.payment_transactions, payment_data)

