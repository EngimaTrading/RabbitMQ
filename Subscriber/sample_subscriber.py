import pika
import argparse

def callback(ch, method, properties, body):
    print(f"Received message: {body}")

def create_connection():
    credentials = pika.PlainCredentials('guest', 'guest')
    parameters = pika.ConnectionParameters('localhost', 5672, '/', credentials)
    connection = pika.BlockingConnection(parameters)
    return connection

def start_subscriber(queue_name):
    connection = create_connection()
    channel = connection.channel()
    channel.queue_declare(queue=queue_name)
    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
    print(f"Waiting for messages from queue {queue_name}. To exit press CTRL+C")
    channel.start_consuming()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Subscribe to a RabbitMQ queue')
    parser.add_argument('queue_name', type=str, help='The name of the queue to subscribe to')
    args = parser.parse_args()
    queue_name = args.queue_name
    start_subscriber(queue_name)
