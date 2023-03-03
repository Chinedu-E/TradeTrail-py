import pika
from pika.adapters.blocking_connection import BlockingChannel
from decouple import config


RABBIT_CONNECTION = pika.BlockingConnection(pika.URLParameters(config('RABBIT_URL')))

def spawn_channel(queue_name: str) -> BlockingChannel:
    channel = RABBIT_CONNECTION.channel()
    channel.queue_declare(queue=queue_name)
    return channel
