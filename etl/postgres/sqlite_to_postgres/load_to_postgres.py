from psycopg2.extensions import connection as _connection
from dataclasses import dataclass as _dataclass
import logging


def postgres_clear_table(connection: _connection, data_description: _dataclass):
    try:
        with connection.cursor() as cursor:
            # Очищаем таблицу в БД, чтобы загружать данные в пустую таблицу
            logging.info(f'Таблица {data_description.schema + "." + data_description.table}')

    except Exception as e:
        logging.exception(f"Произошла ошибка при отчистки таблицы из PostgreSQL, текст ошибки ниже: \n {e}")


def postgres_load_data(connection: _connection, data_description: _dataclass):
    try:
        with connection.cursor() as cursor:
            # Формируем строку для вставки с нужным кол-вом элементов
            insert_string = '(%s'

            for i in range(len(data_description.sqlite_data[0]) - 1):
                insert_string = insert_string + ', %s'

            insert_string = insert_string + ')'

            # Вставляем данные
            args = ','.join(cursor.mogrify(insert_string, item).decode() for item in data_description.sqlite_data)
            cursor.execute(f"""INSERT INTO {data_description.schema}.{data_description.table} ({data_description.column_names}) VALUES {args} ON CONFLICT DO NOTHING""")

    except Exception as e:
        logging.exception(f"Произошла ошибка при записи данных из PostgreSQL, текст ошибки ниже: \n {e}")
