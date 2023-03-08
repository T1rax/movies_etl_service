import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass as _dataclass
from psycopg2.extensions import connection as _connection
from postgres.sqlite_to_postgres.load_to_postgres import postgres_load_data, postgres_clear_table
import logging


@contextmanager
def conn_context(db_path: str):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    yield conn
    conn.close()


def get_sqlite_data_and_load_to_postgres(connection: sqlite3.Connection, pg_conn: _connection, data_description: _dataclass) -> list:
    try:
        curs = connection.cursor()
        curs.execute(f"SELECT * FROM {data_description.table};")
        column_names = list(map(lambda x: x[0], curs.description))
        data_description.column_names = ', '.join(column_names)

        postgres_clear_table(pg_conn, data_description)

        while True:
            rows = curs.fetchmany(1000)
            if not rows:
                break
            
            data_description.sqlite_data = rows
            postgres_load_data(pg_conn, data_description)
        
        logging.info('Данные загружены \n')

    except Exception as e:
        logging.exception("Произошла ошибка при чтении данных из SQLite, текст ошибки ниже: \n %s", e)
