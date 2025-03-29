from finance.data_migratior.migrate import migrate_data
from finance.pg_sql.setup import DBSetup


def main(payment_data_file_name: str):
    DBSetup.setup()
    migrate_data(payment_data_file_name)


if __name__ == '__main__':
    payment_data_file = '/home/ns290/downloads/AC-102024 - octAcct Statement_XX8550_24022025 (1).csv'
    main(payment_data_file)





