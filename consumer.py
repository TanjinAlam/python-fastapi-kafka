# import asyncio
# import os
# import json
# from enum import Enum
# from time import sleep
# from kafka import KafkaConsumer

# # channel
# topic = 'app'

# # consumer
# consumer = KafkaConsumer(topic, bootstrap_servers=[
#                          'localhost:9092'], auto_offset_reset='latest', value_deserializer=lambda x: json.loads(x.decode('utf-8')))


# def write_to_file(file, value):
#     with open(f"{file}.json", "r+") as file:
#         data = json.load(file)
#         data['data'].append(value)
#         file.seek(0)
#         json.dump(data, file)


# async def consumer():
#     try:
#         # async for msg in consumer:
#         #     print(f"Consumed msg: {msg}")
#         #     await asyncio.sleep(30)  # Sleep for 30 seconds
#         async for message in consumer:
#             await asyncio.sleep(30)
#             print("Consuming.....")
#             print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
#                                                 message.offset, message.key, message.value))
#             if message.key == b'create_product':
#                 write_to_file(file="product", value=message.value)
#                 print("Product written to file successfuly.")

#             if message.key == b'create_data':
#                 write_to_file(file="data", value=message.value)
#                 print("Data written to file successfuly.")
#     finally:
#         await consumer.stop()

# for message in consumer:
#     print("Consuming.....")
#     print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
#                                          message.offset, message.key, message.value))
#     if message.key == b'create_product':
#         write_to_file(file="product", value=message.value)
#         print("Product written to file successfuly.")

#     if message.key == b'create_data':
#         write_to_file(file="data", value=message.value)
#         print("Data written to file successfuly.")
import asyncio
import os
import json
from enum import Enum
from time import sleep
import asyncio
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

# channel
topic = 'app'
topicAKG = 'back'
# consumer
# consumer = AIOKafkaConsumer(topic, bootstrap_servers=[
#                          'localhost:9092'], auto_offset_reset='latest', value_deserializer=lambda x: json.loads(x.decode('utf-8')))

# # producer
# consumer_producer = AIOKafkaProducer(bootstrap_servers=[
#                          'localhost:9092'], value_serializer=lambda x: json.dumps(x).encode('utf-8'))

def write_to_file(file, value):
    with open(f"{file}.json", "r+") as file:
        data = json.load(file)
        data['data'].append(value)
        file.seek(0)
        json.dump(data, file)


def on_send_success(record_metadata):
    print(record_metadata.topic)
    print(record_metadata.partition)
    print(record_metadata.offset)


def on_send_error(excp):
    # log.error('I am an errback', exc_info=excp)
    # handle exception
    pass

# async def send_message_async(message):
#     await consumer_producer.send(topic, key=str.encode('UTF-8'), value=message.value)

# 


# async def consume_messages():
#     try:
#         while True:
#             message = await asyncio.to_thread(consumer.__next__)
#             # await asyncio.sleep(30)
#             print("Consuming.....")
#             print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
#                                                   message.offset, message.key, message.value))
#             print("sending from consumer to producer")
#             future = consumer_producer.send(topicAKG, key="HI".encode('UTF-8'), value=message.value)
#             future.add_callback(on_send_success)
#             future.add_errback(on_send_error)
#     finally:
#         consumer.close()
#         consumer_producer.close()

# async def consume_messages():
#     try:
#         while True:
#             message = await asyncio.to_thread(consumer.__next__)
#             # await asyncio.sleep(30)
#             print("Consuming.....")
#             print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
#                                                   message.offset, message.key, message.value))
#             # if message.key == b'create_product':
#             #     write_to_file(file="product", value=message.value)
#             #     print("Product written to file successfully.")

#             # if message.key == b'create_data':
#             #     write_to_file(file="data", value=message.value)
#             #     print("Data written to file successfully.")
#             print("sending from consumer to producer")
#             # async with KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x: json.dumps(x).encode('utf-8')) as response_producer:
#             #     await response_producer.send(topicAKG, value=message.value)
#             # consumer_producer.send(topic, key=str.encode('UTF-8'), value=message.value).add_callback(
#             # on_send_success).add_errback(on_send_error)
#             future = consumer_producer.send(topic, key=str.encode('UTF-8'), value=message.value)
#             future.add_callback(on_send_success)
#             future.add_errback(on_send_error)
#     finally:
#         consumer.close()
#         # consumer_producer.close()


# def run_consumer():
#     loop = asyncio.get_event_loop()
#     try:
#         # loop.create_task(consume_messages())
#         loop.run_until_complete(consume_messages())
#     except KeyboardInterrupt:
#         pass
#     finally:
#         loop.close()


# if __name__ == '__main__':
#     print("CONSUMER IS RUNNING...")
#     run_consumer()
loop = asyncio.get_event_loop()
loop.run_until_complete(consume_messages())