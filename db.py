import pymysql

timeout = 10
connection = pymysql.connect(
    charset="utf8mb4",
    connect_timeout=timeout,
    cursorclass=pymysql.cursors.DictCursor,
    db="defaultdb",
    host="ethindia-mysql-tarushgupta9876-35da.i.aivencloud.com",
    password="AVNS_aN2pZ5xJAsHvJyIiAS3",
    read_timeout=timeout,
    port=16632,
    user="avnadmin",
    write_timeout=timeout,
)


class Database:
    def __init__(self, connection: pymysql.connections.Connection):
        self.connection = connection
        self.cursor = self.connection.cursor()

    def execute(self, query: str, params: tuple = ()):
        self.cursor.execute(query, params)

    def read(self, query: str, params: tuple = ()):
        self.execute(query, params)
        return self.cursor.fetchall()

    def write(self, query: str, params: tuple = ()):
        self.execute(query, params)
        self.connection.commit()


def read_db(query: str, params: tuple = ()):
    return Database(connection).read(query, params)


def write_db(query: str, params: tuple = ()):
    return Database(connection).write(query, params)
