# -*- coding: utf-8 -*-
# Author: Konstantinos Livieratos <livieratos.konstantinos@gmail.com>

from django.conf import settings
from kafka import KafkaConsumer

# Use the KafkaConsumer class to consume latest messages and auto-commit offsets
consumer = KafkaConsumer('requests',
                         bootstrap_servers=settings.KAFKA_SERVERS)
for message in consumer:
    print ("%s:%d:%d: key=%s value=%s" % (
        message.topic.decode('utf-8'), message.partition.decode('utf-8'),
        message.offset.decode('utf-8'), message.key.decode('utf-8'),
        message.value.decode('utf-8'))
           )

# Stop iterating KafkaConsumer if no message can be found after 1sec
KafkaConsumer(consumer_timeout_ms=1000)
