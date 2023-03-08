from postgres.sqlite_to_postgres.load_data import start_load
from datetime import datetime
import logging


def prepare_postgres_tables(pg_conn, state):
    #Создаем таблицы и схему если их нет
    logging.info('Создаем таблицы и схему если их нет')
    with pg_conn.cursor() as cursor:
        cursor.execute(open("postgres/sqlite_to_postgres/movies_database.ddl", "r").read())

    pg_conn.commit()
    
    logging.info('Есть все нужные таблицы')
    
    logging.info('Проверяю есть ли данные в content.film_work')
    # Если таблицы пустые, то переливаем данные из SQLite
    with pg_conn.cursor() as cursor:
        cursor.execute("select count(*) as cnt from content.film_work;")    
        result = cursor.fetchone()
    
    if result['cnt'] == 0:
        logging.info('Таблица content.film_work пустая. Запускаю переливку данных')
        state.set_state('max_fw_upd_at', datetime.timestamp(datetime(2000, 1, 1, 0, 0)))
        start_load()   

    logging.info('Данные из SQLite присутствуют в PostgreSQL')