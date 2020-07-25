import asyncio

from confluent_kafka import Consumer


async def consume(topic_name):
    consumer = Consumer(
        {
            'bootstrap.servers': 'PLAINTEXT://localhost:9092'
            , 'auto.offset.reset': 'earliest'
            , 'group.id': '0',
        }
    )
    
    consumer.subscribe([topic_name])
    
    while True:
        messages = consumer.consume()
        
        for message in messages:
            if message is None:
                print("No message was found!")
            elif message.error() is not None:
                print(f"Message error: {message.error()}.")
            else:
                print(f"{message.value()}")
                
        await asyncio.sleep(1.0)
        
def execute_consumer():
    asyncio.run(consume("sanfran.police.calls"))


if __name__ == "__main__":
    execute_consumer()