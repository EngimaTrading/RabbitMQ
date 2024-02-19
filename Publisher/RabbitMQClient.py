import pika
import logging
import os
import time
import threading
import psutil
import matplotlib.pyplot as plt
import concurrent.futures


class RabbitMQClient:
    def __init__(self, host='localhost', port=5672, virtual_host='/', username='guest', password='guest'):
        self.host = host
        self.port = port
        self.virtual_host = virtual_host
        self.username = username
        self.password = password
        self.connection = None
        self.channel = None

    def connect(self):
        credentials = pika.PlainCredentials(self.username, self.password)
        parameters = pika.ConnectionParameters(
            host=self.host,
            port=self.port,
            virtual_host=self.virtual_host,
            credentials=credentials,
            heartbeat=600  # Adjust heartbeat interval as needed
        )

        logging.info('Connecting to RabbitMQ server...')
        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()

    def declare_exchange(self, exchange_name, exchange_type='direct'):
        self.channel.exchange_declare(exchange=exchange_name, exchange_type=exchange_type)

    def declare_queue(self, queue_name):
        self.channel.queue_declare(queue=queue_name)

    def bind_queue(self, queue_name, exchange_name, routing_key=''):
        self.channel.queue_bind(queue=queue_name, exchange=exchange_name, routing_key=routing_key)

    def publish_message(self, exchange_name, routing_key, message):
        self.channel.basic_publish(exchange=exchange_name, routing_key=routing_key, body=message)

    def consume_queue(self, queue_name, callback):
        self.channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
        self.channel.start_consuming()

    def close_connection(self):
        if self.connection and self.connection.is_open:
            self.connection.close()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    # You can customize the connection parameters based on your setup
    # For example, you can use environment variables to configure the connection
    host = os.getenv('RABBITMQ_HOST', 'localhost')
    port = int(os.getenv('RABBITMQ_PORT', '5672'))
    virtual_host = os.getenv('RABBITMQ_VIRTUAL_HOST', '/')
    username = os.getenv('RABBITMQ_USERNAME', 'guest')
    password = os.getenv('RABBITMQ_PASSWORD', 'guest')

    client = RabbitMQClient(host=host, port=port, virtual_host=virtual_host, username=username, password=password)
    client.connect()

    # Example usage
    exchange_name = 'test_exchange'
    queue_name = 'test_queue'
    routing_key = 'test_routing_key'
    message = 'Hello, RabbitMQ!'

    client.declare_exchange(exchange_name)
    client.declare_queue(queue_name)
    client.bind_queue(queue_name, exchange_name, routing_key)
    client.publish_message(exchange_name, routing_key, message)

    def callback(ch, method, properties, body):
        print("Received message:", body.decode())

    client.consume_queue(queue_name, callback)




# def generate_metrics_graphs(cpu_data, memory_data, disk_io_data):
#     output_file = 'metrics.png'
#     plt.figure(figsize=(12, 8))
#
#     # Plot CPU Usage
#     plt.subplot(3, 1, 1)
#     plt.plot(cpu_data)
#     plt.ylabel('CPU Usage (%)')
#     plt.title('System Metrics Over Time')
#
#     # Plot Memory Usage
#     plt.subplot(3, 1, 2)
#     plt.plot(memory_data)
#     plt.ylabel('Memory Usage (%)')
#
#     # Plot Disk I/O
#     plt.subplot(3, 1, 3)
#     plt.plot(disk_io_data)
#     plt.xlabel('Time (s)')
#     plt.ylabel('Disk I/O (Bytes)')
#
#     plt.savefig(output_file)
#
# def measure_metrics(cpu_usage_data, memory_usage_data, disk_io_data):
#     while True:
#         # Measure CPU usage
#         cpu_percent = psutil.cpu_percent(interval=0.1)
#         cpu_usage_data.append(cpu_percent)
#
#         # Measure memory usage
#         memory_percent = psutil.virtual_memory().percent
#         memory_usage_data.append(memory_percent)
#
#         # Measure disk I/O
#         disk_io = psutil.disk_io_counters()
#         disk_io_data.append(disk_io.write_bytes + disk_io.read_bytes)
#
#         time.sleep(0.1)
#
#
# def read_and_publish_logs(file_path, exchange_name='logs', routing_key='default'):
#     cpu_usage_data = []
#     memory_usage_data = []
#     disk_io_data = []
#
#     # Start a separate thread for measuring metrics
#     metrics_thread = threading.Thread(target=measure_metrics, args=(cpu_usage_data, memory_usage_data, disk_io_data))
#     metrics_thread.start()
#
#     try:
#         connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
#         channel = connection.channel()
#
#         # Declare the exchange if not already declared
#         channel.exchange_declare(exchange=exchange_name, exchange_type='direct')
#
#         with open(file_path, 'r') as file:
#             for line in file:
#                 # Stream each line to RabbitMQ with a routing key
#                 json_data = json.dumps({"log_line": line.strip()})
#                 channel.basic_publish(exchange=exchange_name, routing_key=routing_key, body=json_data)
#                 logging.info(f" [x] Sent JSON: {json_data} with routing key: {routing_key}")
#
#     except Exception as e:
#         logging.error(f"Error in main process: {e}")
#
#     finally:
#         connection.close()
#
#     return cpu_usage_data, memory_usage_data, disk_io_data
#
# if __name__ == '__main__':
#     logging.basicConfig(filename='log_streamer.log', level=logging.INFO)
#     log_file_paths = ['/home/ajain/us_frontend-20231222.log', '/home/ajain/us_fly1-20240119.log', '/home/ajain/us_crude-20240119.log']
#     exchange_name = 'logs'
#
#     routing_keys = ['subscriber1', 'subscriber2', 'subscriber3']
#
#     with concurrent.futures.ThreadPoolExecutor() as executor:
#         futures = [executor.submit(read_and_publish_logs, log_file_path, exchange_name, routing_key) for log_file_path, routing_key in zip(log_file_paths, routing_keys)]
#
#         # Wait for all threads to complete
#         concurrent.futures.wait(futures)
#
#         # Get the results from each thread
#         results = [future.result() for future in futures]
#
#     cpu_data, memory_data, disk_io_data = zip(*results)
#
#     generate_metrics_graphs(cpu_data[0], memory_data[0], disk_io_data[0])
#     generate_metrics_graphs(cpu_data[1], memory_data[1], disk_io_data[1])
#     generate_metrics_graphs(cpu_data[2], memory_data[2], disk_io_data[2])