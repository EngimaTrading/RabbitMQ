import asyncio
import logging
import sys
import time

import aiofiles
from aio_pika import connect, Message, ExchangeType, exceptions


# TODOO

# 1. Handle The Closing Of Connection Gracefully, Already handling Interruption but Need To Address All Corner Cases
# As Well

# 2. Also need To See as I Added The Config Auto Delete As False, And Position File Needs To be Reconsidered And All
# The Thresholds Depending On The Cases

# 3. Also Need To See If The File Is Not Getting Updated, Need To Check The File Issues

# 4. Also One Case Like Is Of Inflight Messages We Want to amke it as robust as possibble to not drop message the
# queue is robust enough But In This Method We Currently Do not If The Subscriber Is Down And The Publisher Is Up,
# The Messages May Be Lost

class PositionManager:
    @staticmethod
    async def save_position(last_position: int, position_file: str) -> None:
        try:
            async with aiofiles.open(position_file, 'w') as file:
                await file.write(str(last_position))
        except Exception as e:
            logging.error(f"Error saving position: {e}")

    @staticmethod
    async def read_position(position_file: str) -> int:
        try:
            async with aiofiles.open(position_file, 'r') as file:
                last_position = int((await file.read()).strip())
                return last_position
        except FileNotFoundError:
            await PositionManager.save_position(0, position_file)
            return 0


class AsyncPublisher:
    def __init__(self, log_file_path: str, queue_name: str):
        self.log_file_path = log_file_path
        self.queue_name = queue_name
        self.connection = None
        self.channel = None

    async def establish_connection(self) -> None:
        while True:
            try:
                self.connection = await connect("amqp://guest:guest@localhost/")
                self.channel = await self.connection.channel()
                await self.channel.declare_exchange(self.queue_name, ExchangeType.DIRECT)
                await self.channel.declare_queue(self.queue_name, auto_delete=False)
                break
            except exceptions.AMQPError as e:
                logging.error(f"Connection failed. Retrying in 5 seconds... Error: {e}")
                await asyncio.sleep(5)

    async def publish_block(self, block: list) -> None:
        for message in block:
            await self.channel.default_exchange.publish(
                Message(message.encode()),
                routing_key=self.queue_name,
            )

    async def process_lines(self, file, last_position, lines_sent, max_lines_per_save, last_save_time):
        while True:
            where = await file.tell()
            line = await file.readline()

            if not line:
                await asyncio.sleep(1)
                break

            message = line.strip()
            if message:
                await self.channel.default_exchange.publish(
                    Message(message.encode()),
                    routing_key=self.queue_name,
                )
                lines_sent += 1
                last_position = await file.tell()

                if lines_sent >= max_lines_per_save:
                    await PositionManager.save_position(last_position, f"{self.queue_name}_position.txt")
                    last_save_time = time.time()
                    lines_sent = 0

        return last_position, lines_sent, last_save_time

    async def close_connection(self) -> None:
        if self.channel is not None:
            await self.channel.close()
        if self.connection is not None:
            await self.connection.close()

    async def monitor_and_publish(self) -> None:
        last_position = await PositionManager.read_position(f"{self.queue_name}_position.txt")
        last_publish_time = time.time()
        last_save_time = time.time()
        lines_sent = 0
        max_lines_per_save = 500  # Adjust as needed

        async with aiofiles.open(self.log_file_path, 'r') as file:
            while True:
                try:
                    await file.seek(last_position)
                    lines_sent = 0

                    last_position, lines_sent, last_save_time = await self.process_lines(file, last_position,
                                                                                         lines_sent, max_lines_per_save,
                                                                                         last_save_time)

                    # Check for new data every 5 minutes
                    if time.time() - last_publish_time > 300:
                        print("Warning: No new data coming in the last 5 minutes.")
                        last_publish_time = time.time()

                    # Save the last position to the file every 2 minutes
                    if time.time() - last_save_time > 120:
                        await PositionManager.save_position(last_position, f"{self.queue_name}_position.txt")
                        last_save_time = time.time()

                except IOError as e:
                    logging.error(f"Error: Could not read the log file. Details: {e}")
                    await asyncio.sleep(5)  # Retry after 5 seconds
                except exceptions.AMQPError as e:  # Handled For network issues
                    logging.error(f"Connection lost. Retrying in 5 seconds... Error: {e}")
                    await asyncio.sleep(5)  # wait before trying to reconnect
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

    publisher = AsyncPublisher(log_file_path, queue_name)
    await publisher.establish_connection()
    await asyncio.create_task(publisher.monitor_and_publish())


if __name__ == "__main__":
    publisher = None
    try:
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
