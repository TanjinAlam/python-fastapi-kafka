import asyncio
import os
import json
from enum import Enum
from time import sleep
import time
from kafka import KafkaProducer

# channel
topic = 'app'

# producer
producer = KafkaProducer(bootstrap_servers=[
                         'localhost:9092'], value_serializer=lambda x: json.dumps(x).encode('utf-8'))


def on_send_success(record_metadata):
    print(record_metadata.topic)
    print(record_metadata.partition)
    print(record_metadata.offset)


def on_send_error(excp):
    # log.error('I am an errback', exc_info=excp)
    # handle exception
    pass


async def publish(method: str, body):
    print(body)
    for i in body: 
        print(f'iteration{i}')
        await waiter()
        data = {"item_name": "piash", "item_price": 123,
        "item_description": "ASD"}
        producer.send(topic, key=method.encode('UTF-8'), value=data).add_callback(
            on_send_success).add_errback(on_send_error)
        print(f'Topic :{topic}  Key :{method}   published.')

    # block until all async messages are sent
    producer.flush()

async def waiter():
    await asyncio.sleep(20)

