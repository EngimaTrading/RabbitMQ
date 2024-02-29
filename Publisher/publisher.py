import asyncio
import logging
import queue
import sys
import time
from datetime import datetime

import aiofiles
from aio_pika import connect, Message, ExchangeType, exceptions


# TODOO

# 1. Handle The Closing Of Connection Gracefully, Already handling Interruption but Need To Address All Corner Cases
# As Well

# 2. Also need To See as I Added The Config Auto Delete As False, And Position File Needs To be Reconsidered And All
# The Thresholds Depending On The Cases

# 3. Also Need To See If The File Is Not Getting Updated, Need To Check The File Issues

# 4. Also One Case Like Is Of Inflight Messages We Want to amke it as robust as possible to not drop message the
# queue is robust enough But In This Method We Currently Do not If The Subscriber Is Down And The Publisher Is Up,
# The Messages May Be Lost

# class DataQueue:
#     """
#     This class is responsible for creating a queue for the publisher and the worker threads.
#     """

#     def __init__(self):
#         self.queue = asyncio.Queue()

#     async def put(self, message: str) -> None:
#         await self.queue.put(message)

#     async def get(self) -> str:
#         return await self.queue.get()

#     def task_done(self) -> None:
#         self.queue.task_done()


class PositionManager:
    """
    This class is responsible for managing the position of the last read line in the log file.
    """

    @staticmethod
    async def save_position(last_position: int, position_file: str) -> None:
        """
        This method is responsible for saving the last position to a file.
        :param last_position:
        :param position_file:
        :return:
        """
        try:
            async with aiofiles.open(position_file, 'w') as file:
                await file.write(str(last_position))
        except Exception as e:
            logging.error(f"Error saving position: {e}")

    @staticmethod
    async def read_position(position_file: str) -> int:
        """
        This method is responsible for reading the last position from a file.
        :param position_file:
        :return:
        """
        try:
            async with aiofiles.open(position_file, 'r') as file:
                last_position = int((await file.read()).strip())
                return last_position
        except FileNotFoundError:
            await PositionManager.save_position(0, position_file)
            return 0


class FileReader:
    """
    This class is responsible for reading the log file and writing to the DataQueue.
    """

    def __init__(self, log_file_path: str, queue_name: str, data_queue):
        self.log_file_path = log_file_path
        self.queue_name = queue_name
        self.data_queue = data_queue

    async def process_lines(self, file, last_position, lines_sent, max_lines_per_save, last_save_time):
        while True:
            where = file.tell()
            line = file.readline()

            if not line:
                await asyncio.sleep(0)
                break

            if self.data_queue.qsize() > 100:
                await asyncio.sleep(0)
                continue

            message = line.strip()
            if message:
                log_type = 'NONE'
                msg_type = 'NONE'

                if 'INFO:' in message:
                    log_type = 'INFO'

                if '|MD|' in message:
                    msg_type = 'MD'

                await self.data_queue.put((message, log_type, msg_type))
                lines_sent += 1
                last_position = file.tell()

                # if lines_sent >= max_lines_per_save:
                #     await PositionManager.save_position(last_position, f"{self.queue_name}_position.txt")
                #     last_save_time = time.time()
                #     lines_sent = 0

        return last_position, lines_sent, last_save_time,

    async def read_file(self) -> None:
        """
        This method is responsible for monitoring the log file and publishing new lines to the queue. Also, it saves
        the last position to a file every 2 minutes. We Will Decide On the Thresholds Later
        :return:
        """
        last_position = 0  # PositionManager.read_position(f"{self.queue_name}_position.txt")
        last_publish_time = time.time()
        last_save_time = time.time()
        lines_sent = 0
        max_lines_per_save = 500  # Adjust as needed

        with open(self.log_file_path, 'r') as file:
            while True:
                try:
                    file.seek(last_position)
                    lines_sent = 0

                    print('processing lines')
                    last_position, lines_sent, last_save_time = await self.process_lines(file, last_position,
                                                                                         lines_sent, max_lines_per_save,
                                                                                         last_save_time)

                    # print(f"Last position: {last_position}")

                    # Check for new data every 5 minutes
                    if time.time() - last_publish_time > 300:
                        print("Warning: No new data coming in the last 5 minutes.")
                        last_publish_time = time.time()

                    # Save the last position to the file every 2 minutes
                    if time.time() - last_save_time > 120:
                        # PositionManager.save_position(last_position, f"{self.queue_name}_position.txt")
                        last_save_time = time.time()

                    await asyncio.sleep(0)

                except IOError as e:
                    logging.error(f"Error: Could not read the log file. Details: {e}")
                    await asyncio.sleep(5)  # Retry after 5 seconds


class AsyncPublisher:
    """
    This class is responsible for Publishing messages From a local queue.
    """

    def __init__(self, queue_name: str, data_queue):
        self.queue_name = queue_name
        self.connection = None
        self.channel = None
        self.data_queue = data_queue

    async def establish_connection(self) -> None:
        """
        This method is responsible for establishing a connection to the RabbitMQ server.
        :return:
        """
        while True:
            try:
                self.connection = await connect("amqp://guest:guest@localhost/")
                self.channel = await self.connection.channel()
                # await self.channel.queue_delete(self.queue_name)
                self.topic_exchange = await self.channel.declare_exchange('enigma_logs', ExchangeType.TOPIC,
                                                                          auto_delete=False)
                break
            except exceptions.AMQPError as e:
                logging.error(f"Connection failed. Retrying in 5 seconds... Error: {e}")
                await asyncio.sleep(5)

    async def publish_block(self, block: list) -> None:
        """
        This method is responsible for publishing a block of messages to the queue.
        :param block:
        :return:
        """
        for message, log_type, msg_type in block:
            queue_name = f'{self.queue_name}.{log_type}.{msg_type}'
            try:
                await self.topic_exchange.publish(Message(message.encode()), routing_key=queue_name)
            except Exception as e:
                print(f"Error during publish: {e}")

    async def start_publishing(self) -> None:
        """
        This method is responsible for starting the publishing of messages to the queue.
        :return:
        """
        block = []
        while True:
            try:
                message, log_type, msg_type = await self.data_queue.get()
                block.append((message, log_type, msg_type))
                if len(block) >= 100:
                    await self.publish_block(block)
                    block = []
                    self.data_queue.task_done()
            except queue.Empty:
                if block:
                    await self.publish_block(block)
                    block = []
                await asyncio.sleep(0.01)
            except Exception as e:
                print(f"Error: {e}")

        print('end!!!')

    async def close_connection(self) -> None:
        """
        This method is responsible for closing the connection to the RabbitMQ server.
        :return:
        """
        if self.channel is not None:
            await self.channel.close()
        if self.connection is not None:
            await self.connection.close()


async def main() -> None:
    global publisher
    if len(sys.argv) < 3:
        print("Usage: python your_script.py <trade_name> <date>")
        sys.exit(1)

    trade_name = sys.argv[1]
    date = sys.argv[2]

    # log_file_path = f"/home/ajain/Analysis/{trade_name}-{date}.log"
    log_file_path = f'/Users/hcorra/data/logs/brz_mid-20240117.log'
    queue_name = trade_name

    data_queue = asyncio.Queue()
    file_reader = FileReader(log_file_path, queue_name, data_queue)
    publisher = AsyncPublisher(queue_name, data_queue)
    await publisher.establish_connection()
    print('connection established!')
    task_read = asyncio.create_task(file_reader.read_file())
    print('creating task start_publishing')
    task_publish = asyncio.create_task(publisher.start_publishing())

    await task_read
    await task_publish


if __name__ == "__main__":
    publisher = None
    try:
        print('Starting Publisher')
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Program terminated by user.")
        if publisher is not None:
            loop = asyncio.get_event_loop()
            loop.run_until_complete(publisher.close_connection())
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        if publisher is not None:
            loop = asyncio.get_event_loop()
            loop.run_until_complete(publisher.close_connection())