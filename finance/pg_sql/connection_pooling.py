from psycopg2.pool import SimpleConnectionPool

from finance.configs import DBConfig, config


__all__ = [
    'pool'
]


class ConnectionPool:
    def __init__(
            self,
            db_name: str,
            db_user: str,
            db_password: str,
            db_host: str,
            db_port: str,
            min_pool_connections: int = 1,
            max_pool_connections: int = 10
    ):
        self.db_name = db_name
        self.db_user = db_user
        self.db_password = db_password
        self.db_host = db_host
        self.db_port = db_port

        self._min_pool_connection = min_pool_connections
        self._max_pool_connection = max_pool_connections
        self._setup_connection_pool()  # delegate pool creation out of init

    def _setup_connection_pool(self):
        self._connection_pool = SimpleConnectionPool(
            self._min_pool_connection,
            self._max_pool_connection,
            dbname=self.db_name,
            user=self.db_user,
            password=self.db_password,
            host=self.db_host,
            port=self.db_port,
        )

    @classmethod
    def from_config(cls, db_config: DBConfig):
        return cls(**db_config.to_dict())._connection_pool


pool = ConnectionPool.from_config(config)








