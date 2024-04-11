from dataclasses import dataclass, asdict


@dataclass
class DBConfig:
    db_name: str
    db_user: str
    db_password: str
    db_host: str
    db_port: str
    min_pool_connections: int = 1
    max_pool_connections: int = 10

    def to_dict(self):
        return asdict(self)


@dataclass
class DBTableNames:
    payment_transactions = 'testststs'


@dataclass
class DBTableQueries:
    payments: str = f"""
            CREATE TABLE {DBTableNames.payment_transactions} (
            narration VARCHAR(255),
            value_dt DATE,
            withdraw_amt DECIMAL(10, 2),
            deposit_amt DECIMAL(10, 2),
            closing_balance DECIMAL(10, 2),
            ref_number VARCHAR(255),
            datestamp DATE
        );
        CREATE UNIQUE INDEX payment_transactions_ref_number_unique_idx
        ON {DBTableNames.payment_transactions} (ref_number)
        WHERE ref_number != '000000000000000';
    """

    def to_dict(self):
        return asdict(self)


config = DBConfig(
    db_name='postgres',
    db_user='postgres',
    db_password='admin123',
    db_host='localhost',
    db_port='5432'
)
