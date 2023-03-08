import sqlite3
from dataclasses import dataclass, field
import psycopg2
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor
import os

from postgres.sqlite_to_postgres.get_sqlite_data import get_sqlite_data_and_load_to_postgres


@dataclass
class DataDescription:
    schema: str
    table: str
    column_names: str = field(default=None)
    sqlite_data: list = field(default=None)


def load_from_sqlite(connection: sqlite3.Connection, pg_conn: _connection, tables_for_migration: list, schema: str):
    """Основной метод загрузки данных из SQLite в Postgres"""

    # Функции ниже полностью сотрут данные в указанных таблицах и зальют туда данные из SQLite
    for table in tables_for_migration:
        data_description = DataDescription(schema=schema, table=table)
        get_sqlite_data_and_load_to_postgres(connection, pg_conn, data_description)


def start_load():
    dsl = {'dbname': os.environ.get('DB_NAME'), 'user': os.environ.get('DB_USER'), 'password': os.environ.get('DB_PASSWORD'), 'host': os.environ.get('DB_HOST'), 'port': os.environ.get('DB_PORT')}

    schema = 'content'

    tables_for_migration = ['genre', 'film_work', 'person', 'genre_film_work', 'person_film_work']
    try:
        with sqlite3.connect('postgres/sqlite_to_postgres/db.sqlite') as sqlite_conn, psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn:
            load_from_sqlite(sqlite_conn, pg_conn, tables_for_migration, schema)
    finally:
        pg_conn.close()