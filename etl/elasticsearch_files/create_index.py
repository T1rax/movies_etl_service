import elasticsearch
import logging
import backoff

@backoff.on_exception(backoff.expo
                      , (elasticsearch.ConnectionError
                      , elasticsearch.ConnectionTimeout)
                      , max_tries=10)
def es_create_index_if_not_exists(es, index, index_config):
    """Create the given ElasticSearch index and ignore error if it already exists"""
    try:
        if not es.indices.exists(index = index):
            logging.info('Need to create index %s', index)
            es.indices.create(index = index, body = index_config)
            logging.info('Index %s created', index)
        else:
            logging.info('Index %s exists', index)
    
    except elasticsearch.exceptions.RequestError as ex:
        raise ex