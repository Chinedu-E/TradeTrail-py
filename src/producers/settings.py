import pika

RABBIT_CONNECTION = pika.BlockingConnection(pika.ConnectionParameters('localhost'))

def spawn_channel(queue_name: str):
    channel = RABBIT_CONNECTION.channel()
    channel.queue_declare(queue=queue_name)
    return channel
