from elasticsearch.helpers import bulk
import elasticsearch
import logging
import backoff


def es_movies_dict_generator(pg_data):
    """Переводит данные в формат для ES"""

    for row in pg_data:
    
        es_doc = {
            'id': row['id'],
            'imdb_rating': row['imdb_rating'],
            'genre': row['genres'],
            'title': row['title'],
            'description': row['description'],
            'director': [a['person_name'] for a in row['persons'] if a['person_role'] == 'director'],
            'actors_names': [a['person_name'] for a in row['persons'] if a['person_role'] == 'actor'],
            'writers_names': [a['person_name'] for a in row['persons'] if a['person_role'] == 'writer'],
            'actors': [{'id': a['person_id'], 'name': a['person_name']} for a in row['persons'] if a['person_role'] == 'actor'],
            'writers': [{'id': a['person_id'], 'name': a['person_name']} for a in row['persons'] if a['person_role'] == 'writer'],
        }

        es_dict = {
            '_index': 'movies',
            '_id': es_doc['id'],
            '_source': es_doc,
        }

        yield es_dict


@backoff.on_exception(backoff.expo
                      , (elasticsearch.ConnectionError
                      , elasticsearch.ConnectionTimeout)
                      , max_tries=10)
def write_to_movies(pg_data, es):
    """Фактическая заливка данных в ES"""

    res = bulk(es, es_movies_dict_generator(pg_data))

    logging.info('Bulk load result: %s', res)