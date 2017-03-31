# -*- coding: utf-8 -*-
# Author: Konstantinos Livieratos <livieratos.konstantinos@gmail.com>

import logging
from datetime import datetime
from django.conf import settings
from kafka import KafkaConsumer
from elasticsearch import Elasticsearch

_INDEX_NAME = 'requests'

es_client = Elasticsearch(
    hosts=[settings.ES_HOST]
)


def process_es(r_body):
    """
    Updates the ElasticSearch index with request's info.
    Than can be helpful for eg Kibana visualizations. 
    :return: 
    """
    if not es_client.indices.exists(index=_INDEX_NAME):
        mapping = {
            "mappings": {
                "request-info": {
                    "properties": {
                        "timestamp": {
                            "type": "date"
                        },
                        "body": {
                            "type": "string"
                        }
                    }
                }
            }
        }
        logging.info("Creating new ElasticSearch indice...")
        es_client.indices.create(index=_INDEX_NAME, body=mapping)

    doc = {
        "timestamp": datetime.now(),
        "body": str(r_body)
    }

    res = es_client.index(index=_INDEX_NAME, doc_type='request-info', body=doc)
    if res['created']:
        logging.info("new request indexed")


if __name__ == '__main__':
    # Use the KafkaConsumer class to consume latest messages and auto-commit offsets
    # `consumer_timeout_ms` param is used in order to stop iterating KafkaConsumer
    # if no message can be found after 1sec
    consumer = KafkaConsumer(
        'requests',
        bootstrap_servers=settings.KAFKA_SERVERS,
        consumer_timeout_ms=1000
    )
    for message in consumer:
        request_body = message.value.decode('utf-8')
        print ("%s:%d:%d: key=%s value=%s" % (
            message.topic.decode('utf-8'), message.partition.decode('utf-8'),
            message.offset.decode('utf-8'), message.key.decode('utf-8'),
            request_body)
               )
        process_es(request_body)
