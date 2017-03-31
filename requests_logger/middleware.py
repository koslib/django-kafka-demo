# -*- coding: utf-8 -*-
# Author: Konstantinos Livieratos <livieratos.konstantinos@gmail.com>

import logging
from datetime import datetime
from django.conf import settings
from kafka import KafkaProducer


producer = KafkaProducer(
    bootstrap_servers=settings.KAFKA_SERVERS,
    retries=5
)


class RequestLoggerMiddleware(object):
    """
    Transmits all requests' data to Kafka as a simple string.
    !Attention: Demonstration purpose only!
    """

    def process_request(self, request):
        if settings.LOCAL or settings.DEBUG:
            return None
        DISALLOWED_USER_AGENTS = ["ELB-HealthChecker/1.0"]
        http_user_agent = request.environ.get('HTTP_USER_AGENT', '')
        if http_user_agent in DISALLOWED_USER_AGENTS:
            return None

        before = datetime.datetime.now()

        # KafkaProducer is an asynchronous message producer, so I can use it here and
        # it will transmit the request string asynchronously.
        # the `request.body` attribute is already a byte string so it doesn't need type casting or encoding
        producer.send(
            topic='requests',
            key=b'request',
            value=request.body
        )
        after = datetime.datetime.now()

        # log total time taken inside this middleware class
        # to verify that total time is within acceptable range
        logging.info("TotalTimeInRequestLoggerMiddleware %s" % ((after - before).total_seconds()))
        return None
