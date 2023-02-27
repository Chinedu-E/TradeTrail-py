import pika
from pika.adapters.blocking_connection import BlockingChannel

RABBIT_CONNECTION = pika.BlockingConnection(pika.ConnectionParameters('localhost'))

def spawn_channel(queue_name: str) -> BlockingChannel:
    channel = RABBIT_CONNECTION.channel()
    channel.queue_declare(queue=queue_name)
    return channel
