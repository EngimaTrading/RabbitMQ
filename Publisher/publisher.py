import os
import asyncio
from aiofiles import open as aio_open
from aio_pika import connect, Message, ExchangeType, exceptions
from message_parser import parse_message_type
import sys
import logging

# TODOO

# 1. Handle The Closing Of Connection Gracefully, Already handling Interruption but Need To Address All Corner Cases
# As Well

# 2. Also need To See as I Added The Config Auto Delete As False, And Position File Needs To be Reconsidered And All
# The Thresholds Depending On The Cases

# 3. Also Need To See If The File Is Not Getting Updated, Need To Check The File Issues

# 4. Also One Case Like Is Of Inflight Messages We Want to amke it as robust as possible to not drop message the
# queue is robust enough But In This Method We Currently Do not If The Subscriber Is Down And The Publisher Is Up,
# The Messages May Be Lost

class PositionManager:
    @staticmethod
    async def save_position(last_position: int, position_file: str) -> None:
        async with aio_open(position_file, 'w') as file:
            await file.write(str(last_position))

    @staticmethod
    async def read_position(position_file: str) -> int:
        try:
            async with aio_open(position_file, 'r') as file:
                position = await file.read()
                return int(position.strip()) if position.strip() else 0
        except FileNotFoundError:
            return 0

class FileReader:
    def __init__(self, log_file_path: str, queue_name: str, data_queue):
        self.log_file_path = log_file_path
        self.queue_name = queue_name
        self.data_queue = data_queue
        self.last_position = 0
        self.last_file_size = 0

    async def read_file(self) -> None:
        self.last_position = await PositionManager.read_position(f"{self.queue_name}_position.txt")
        async with aio_open(self.log_file_path, 'r') as file:
            await file.seek(self.last_position)
            while True:
                try:
                    current_file_size = os.path.getsize(self.log_file_path)
                    if current_file_size > self.last_file_size:
                        lines_sent = 0
                        while line := await file.readline():
                            message = line.strip()
                            if message:
                                log_type = 'NONE'
                                msg_type = parse_message_type(message)
                                if 'INFO:' in message:
                                    log_type = 'INFO'
                                elif 'ERROR:' in message:
                                    log_type = 'ERROR'
                                await self.data_queue.put((message, log_type, msg_type))
                                lines_sent += 1
                                if lines_sent >= 100:
                                    self.last_position = await file.tell()  # Await the tell method
                                    await PositionManager.save_position(self.last_position, f"{self.queue_name}_position.txt")
                                    lines_sent = 0
                        self.last_file_size = current_file_size
                except IOError as e:
                    logging.error(f"Error: Could not read the log file. Details: {e}")
                    await asyncio.sleep(5)

class AsyncPublisher:
    def __init__(self, queue_name: str, data_queue):
        self.queue_name = queue_name
        self.connection = None
        self.channel = None
        self.data_queue = data_queue

    async def establish_connection(self) -> None:
        while True:
            try:
                self.connection = await connect("amqp://guest:guest@localhost/")
                self.channel = await self.connection.channel()
                self.topic_exchange = await self.channel.declare_exchange('enigma_logs', ExchangeType.TOPIC, auto_delete=False)
                break
            except exceptions.AMQPError as e:
                logging.error(f"Connection failed. Retrying in 5 seconds... Error: {e}")
                await asyncio.sleep(5)

    async def publish_block(self, block: list) -> None:
        for message, log_type, msg_type in block:
            queue_name = f'{self.queue_name}.{log_type}.{msg_type}'
            try:
                await self.topic_exchange.publish(Message(message.encode()), routing_key=queue_name)
            except exceptions.ChannelClosed as e:
                print(f"Channel closed during publish: {e}")
                await self.handle_channel_closed()
            except Exception as e:
                print(f"Error during publish: {e}")

    async def start_publishing(self) -> None:
        block = []
        while True:
            try:
                message, log_type, msg_type = await self.data_queue.get()
                block.append((message, log_type, msg_type))
                if len(block) >= 100:
                    await self.publish_block(block)
                    block = []
                    self.data_queue.task_done()
            except asyncio.QueueEmpty:
                if block:
                    await self.publish_block(block)
                    block = []
                await asyncio.sleep(0.01)
            except Exception as e:
                print(f"Error: {e}")

        print('Exiting start_publishing')

    async def close_connection(self) -> None:
        if self.channel is not None:
            await self.channel.close()
        if self.connection is not None:
            await self.connection.close()

    async def handle_channel_closed(self):
        print("Handling channel closed error...")
        await self.close_connection()
        await self.establish_connection()

async def main() -> None:
    global publisher
    if len(sys.argv) < 3:
        print("Usage: python your_script.py <trade_name> <date>")
        sys.exit(1)

    trade_name = sys.argv[1]
    date = sys.argv[2]

    log_file_path = f"/home/ajain/Analysis/{trade_name}-{date}.log"
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