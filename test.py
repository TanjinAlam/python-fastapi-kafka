# #Producer
# import asyncio
# import json
# from random import randint
# from aiokafka import AIOKafkaProducer, AIOKafkaConsumer

# KAFKA_BOOTSTRAP_SERVERS = 'your_bootstrap_servers'
# KAFKA_TOPIC = 'your_topic'
# ACK_TOPIC = 'ack_topic'

# async def producer():
#     producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
#     await producer.start()

#     try:
#         for i in range(1, 11):
#             msg_id = f'{randint(1, 10000)}'
#             value = {'message_id': msg_id, 'text': 'some text', 'state': randint(1, 100)}
#             print(f'Sending message with value: {value}')
#             value_json = json.dumps(value).encode('utf-8')
#             await producer.send_and_wait(KAFKA_TOPIC, value_json)

#             await asyncio.sleep(30)  # Sleep for 30 seconds before sending the next message
#     finally:
#         await producer.stop()

# loop = asyncio.get_event_loop()
# loop.run_until_complete(producer())
# loop.close()



# #Consumer
# import asyncio
# import json
# from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

# KAFKA_BOOTSTRAP_SERVERS = 'your_bootstrap_servers'
# KAFKA_TOPIC = 'your_topic'
# ACK_TOPIC = 'ack_topic'

# async def consume():
#     consumer = AIOKafkaConsumer(KAFKA_TOPIC, bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
#                                 group_id='consumer_group')
#     await consumer.start()

#     producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
#     await producer.start()

#     try:
#         for msg in consumer:
#             payload = json.loads(msg.value.decode('utf-8'))
#             print(f"Consumed msg: {payload}")

#             await asyncio.sleep(30)  # Sleep for 30 seconds before sending the acknowledgment

#             # Send acknowledgment to producer
#             ack_payload = {'message_id': payload['message_id']}
#             ack_value = json.dumps(ack_payload).encode('utf-8')
#             await producer.send_and_wait(ACK_TOPIC, ack_value)
#     finally:
#         await consumer.stop()
#         await producer.stop()

# loop = asyncio.get_event_loop()
# loop.run_until_complete(consume())
# loop.close()


import asyncio
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
# channel
topic = 'app'
topicAKG = 'back'
# channel


async def consume():
    consumer = AIOKafkaConsumer(topic, bootstrap_servers='localhost:9092',group_id='my_consumer_group')
    await consumer.start()

    producer = AIOKafkaProducer(bootstrap_servers='localhost:9092')
    await producer.start()

    try:
        async for message in consumer:
            print("Received",message.value.decode())
            await asyncio.sleep(3)  # Delay for 3 seconds
            await consumer.commit()  # Commit the offset to avoid re-consuming the same message
            await producer.send_and_wait(topicAKG, value='from consumer'.encode())

    finally:
        await consumer.stop()
        await producer.stop()

loop = asyncio.get_event_loop()
loop.run_until_complete(consume())