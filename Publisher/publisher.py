import os
import time
import threading
import pika
import sys

class PositionManager:
    @staticmethod
    def save_position(last_position, position_file):
        try:
            with open(position_file, 'w') as file:
                file.write(str(last_position))
        except Exception as e:
            print(f"Error saving position: {e}")

    @staticmethod
    def read_position(position_file):
        try:
            with open(position_file, 'r') as file:
                last_position = int(file.read().strip())
                return last_position
        except FileNotFoundError:
            PositionManager.save_position(0, position_file)
            return 0

class Publisher:
    def __init__(self, log_file_path, queue_name, position_file):
        self.log_file_path = log_file_path
        self.queue_name = queue_name
        self.position_file = position_file
        self.connection = self.establish_connection()
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=queue_name)

    def establish_connection(self):
        while True:
            try:
                credentials = pika.PlainCredentials('guest', 'guest')
                parameters = pika.ConnectionParameters('localhost', 5672, '/', credentials)
                connection = pika.BlockingConnection(parameters)
                return connection
            except pika.exceptions.AMQPConnectionError:
                print("Connection failed. Retrying in 5 seconds...")
                time.sleep(5)

    def publish_block(self, block):
        for message in block:
            self.channel.basic_publish(exchange='', routing_key=self.queue_name, body=message)

    # TODO Implement Ack mechanism From Subscriber and Remove Cases Of Duplicative
    def monitor_and_publish(self):
        last_position = PositionManager.read_position(self.position_file)
        last_publish_time = time.time()
        last_save_time = time.time()
        lines_sent = 0
        max_lines_per_save = 100  # Adjust as needed

        while True:
            try:
                with open(self.log_file_path, 'r') as file:
                    file.seek(last_position)
                    lines_sent = 0

                    while True:
                        where = file.tell()
                        line = file.readline()

                        if not line:
                            time.sleep(1)
                            break

                        message = line.strip()
                        if message:
                            self.channel.basic_publish(exchange='', routing_key=self.queue_name, body=message)
                            lines_sent += 1
                            last_position = file.tell()

                            if lines_sent >= max_lines_per_save:
                                PositionManager.save_position(last_position, self.position_file)
                                last_save_time = time.time()
                                lines_sent = 0

                        # Check for new data every 5 minutes
                        if time.time() - last_publish_time > 300:
                            print("Warning: No new data coming in the last 5 minutes.")
                            last_publish_time = time.time()

                        # Save the last position to the file every 2 minutes
                        if time.time() - last_save_time > 120:
                            PositionManager.save_position(last_position, self.position_file)
                            last_save_time = time.time()

            except IOError as e:
                print(f"Error: Could not read the log file. Details: {e}")
                time.sleep(5)  # Retry after 5 seconds


def start_publisher(log_file_path, queue_name, position_file):
    publisher = Publisher(log_file_path, queue_name, position_file)
    monitor_thread = threading.Thread(target=publisher.monitor_and_publish)
    monitor_thread.start()

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python your_script.py <trade_name> <date>")
        sys.exit(1)

    trade_name = sys.argv[1]
    date = sys.argv[2]

    log_file_path = f"/teamdata/enigma/logs/{trade_name}-{date}.log"
    position_file = f"{trade_name}_position.txt"
    queue_name = trade_name
    start_publisher(log_file_path, queue_name, position_file)


