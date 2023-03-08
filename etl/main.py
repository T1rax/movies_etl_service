import json
import os
import logging
import psycopg2
from psycopg2.extras import RealDictCursor
import time
from datetime import datetime, timezone
import backoff
import yaml

from elasticsearch_files.create_index import es_create_index_if_not_exists
from elasticsearch_files.write_to_elastic import write_to_movies
from elasticsearch import Elasticsearch

from file_state.save_sate import State, JsonFileStorage

from postgres.prepare_postgres import prepare_postgres_tables
from postgres.get_postgres_data import get_new_film_updates_cursor, get_new_genre_updates_cursor, get_new_person_updates_cursor

logging.getLogger().setLevel(logging.INFO)

with open("config.yaml", 'r') as file:
   config = yaml.safe_load(file)


def prepare_environment(es):
    """Создание индекса"""

    # Read config files
    with open("config/es_movies_index.json", "r") as jsonfile:
        es_movies_index = json.load(jsonfile)
    
    es_create_index_if_not_exists(es, 'movies', es_movies_index)


def get_ts_for_request(key):
    """Ищет последнюю дату изменения в файлах или создает новую запись"""

    ts = state.get_state(key)

    if ts == None:
        if key == 'max_fw_upd_at':
            ts = datetime.timestamp(datetime(2000, 1, 1, 0, 0))
        else:
            ts = datetime.timestamp(datetime.now(timezone.utc))
        state.set_state(key, ts)
    
    return ts


def pg_cursor_etl_loader(cursor, es, state, ts, state_key):
    """Получает на вход курсор из PG и по чанкам грузит в ES"""
    
    try:
        while True:
            pg_data = cursor.fetchmany(100)
            if pg_data == []:
                break
            else:
                max_pg_ts = datetime.timestamp(max(row['updated_at'] for row in pg_data))
                if max_pg_ts > ts:
                    ts = max_pg_ts
                
                logging.info(' Data from PostgreSQL - Batch size %d', len(pg_data))
                write_to_movies(pg_data, es)

        state.set_state(state_key, ts)
    finally:
        cursor.close()


def etl_process(pg_conn, es, state):
    """Основной файл для запуска"""

    #Get timestamps
    fw_ts = get_ts_for_request('max_fw_upd_at')
    g_ts = get_ts_for_request('max_g_upd_at')
    p_ts = get_ts_for_request('max_p_upd_at')


    # Films load
    pg_cursor_etl_loader(get_new_film_updates_cursor(pg_conn, datetime.fromtimestamp(fw_ts))
                        , es
                        , state
                        , fw_ts
                        , 'max_fw_upd_at')

    # Genres load
    pg_cursor_etl_loader(get_new_genre_updates_cursor(pg_conn, datetime.fromtimestamp(g_ts))
                        , es
                        , state
                        , g_ts
                        , 'max_g_upd_at')

    # Persons load
    pg_cursor_etl_loader(get_new_person_updates_cursor(pg_conn, datetime.fromtimestamp(p_ts))
                        , es
                        , state
                        , p_ts
                        , 'max_p_upd_at')


@backoff.on_exception(backoff.expo
                      , (psycopg2.errors.ConnectionException
                      , psycopg2.errors.SqlclientUnableToEstablishSqlconnection
                      , psycopg2.errors.ConnectionFailure
                      , psycopg2.errors.CrashShutdown
                      , psycopg2.errors.CannotConnectNow)
                      , max_tries=10)
def connect_to_pg_and_execute(dsl, es, state):
    """Отдельно вынесенный коннекшн к базе для ретраев"""
    try:
        pg_conn = psycopg2.connect(**dsl, cursor_factory=RealDictCursor)

        with pg_conn:
            prepare_postgres_tables(pg_conn, state)

            while True:
                logging.info('Собираю новые записи')
                etl_process(pg_conn, es, state)

                logging.info('Перегрузка данных окончена, ожидаю следующий запуск')
                time.sleep(config['next_try_sleep'])

    finally:
        pg_conn.close()


if __name__ == '__main__':

    dsl = {'dbname': os.environ.get('DB_NAME'), 'user': os.environ.get('DB_USER'), 'password': os.environ.get('DB_PASSWORD'), 'host': os.environ.get('DB_HOST'), 'port': os.environ.get('DB_PORT')}
    es = Elasticsearch(os.environ.get('ES_HOST'))

    state = State(JsonFileStorage('etl_date_states.txt'))

    prepare_environment(es)

    connect_to_pg_and_execute(dsl, es, state)