import pika
import random
import string
from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange

def init_pika_connection(host):
    return pika.BlockingConnection(pika.ConnectionParameters(host=host))

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self.connection = init_pika_connection(host)
        self.channel = self.connection.channel()
        self.queue_name = queue_name
        self.current_delivery_callback_tag = None

        self.channel.queue_declare(queue=queue_name, durable=False, exclusive=False, auto_delete=False)

    def ack(self):
        self.channel.basic_ack(delivery_tag=self.current_delivery_callback_tag)

    def nack(self):
        self.channel.basic_nack(delivery_tag=self.current_delivery_callback_tag)

    def handle_pika_delivery(self, channel, method, properties, body):
        self.current_delivery_callback_tag = method.delivery_tag
        self._on_message_callback(body, self.ack, self.nack)

    def send(self, message: bytes):
        self.channel.basic_publish(
            exchange='',
            routing_key=self.queue_name,
            body=message
        )

    def start_consuming(self, on_message_callback):
        self._on_message_callback = on_message_callback
        self.channel.basic_consume(queue=self.queue_name, on_message_callback=self.handle_pika_delivery, auto_ack=False)
        self.channel.start_consuming()

    def stop_consuming(self):
        self.channel.stop_consuming()

    def close(self):
        self.channel.close()
        self.connection.close()

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        self.connection = init_pika_connection(host)
        self.channel = self.connection.channel()
        self.exchange_name = exchange_name

        self.routing_keys = routing_keys

        self.channel.exchange_declare(exchange=exchange_name, exchange_type='direct', durable=True, auto_delete=False)

    def send(self, message: bytes):
        self.channel.basic_publish(
            exchange=self.exchange_name,
            routing_key=self.routing_keys,
            body=message
        )

    def start_consuming(self, on_message_callback):
        pass

    def stop_consuming(self):
        self.channel.stop_consuming()

    def close(self):
        self.channel.close()
        self.connection.close()
    
