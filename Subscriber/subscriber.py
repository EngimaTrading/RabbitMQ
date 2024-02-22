import asyncio
import logging
import signal
from aio_pika import connect, IncomingMessage, exceptions
import sys

# TODOO

# 1. Handle The Closing Of Connection Gracefully, Already handling Interruption but Need To Address All Corner Cases
# As Well

# 2. Also need To See as I Added The Config Auto Delete As False
# Because The Broker Has This Change Need To Check If This needs To be persisted
# This helps In binding The Queue And Not Deleting If kills

# 3. Need To Ack message After Certain interval

# 4. Need To Address Connection Issues

class AsyncSubscriber:
    """
    This class is responsible for consuming messages from a queue.
    """
    def __init__(self, queue_name: str):
        self.queue_name = queue_name
        self.connection = None
        self.channel = None

    async def establish_connection(self) -> None:
        """
        This method is responsible for establishing a connection to the RabbitMQ server.
        :return:
        """
        while True:
            try:
                self.connection = await connect("amqp://guest:guest@localhost/")
                self.channel = await self.connection.channel()
                await self.channel.declare_queue(self.queue_name, auto_delete=False)
                await self.channel.set_qos(prefetch_count=1)
                break
            except exceptions.AMQPError as e:
                logging.error(f"Connection failed. Retrying in 5 seconds... Error: {e}")
                await asyncio.sleep(5)

    async def on_message(self, message: IncomingMessage) -> None:
        """
        This method is called whenever a new message is received.
        :param message:
        :return:
        """
        async with message.process():
            print(f" [x] Received: {message.body.decode()}")

    async def start_consuming(self) -> None:
        """
        This method is responsible for starting the consuming of messages from the queue.
        :return:
        """
        queue = await self.channel.declare_queue(self.queue_name)
        await queue.consume(self.on_message)

    async def run(self) -> None:
        """
        This method is responsible for running the subscriber.
        :return:
        """
        while True:
            try:
                await self.establish_connection()
                await self.start_consuming()
            except Exception as e:
                logging.error(f"An error occurred: {e}")
                await asyncio.sleep(5)  # wait before trying to reconnect

    async def close_connection(self) -> None:
        """
        This method is responsible for closing the connection to the RabbitMQ server.
        :return:
        """
        if self.channel is not None:
            await self.channel.close()
        if self.connection is not None:
            await self.connection.close()

if __name__ == "__main__":

    if len(sys.argv) < 2:
        print("Usage: python your_script.py <queue_name>")
        sys.exit(1)

    queue_name = sys.argv[1]
    # queue_name = "brz_frt3"
    subscriber = AsyncSubscriber(queue_name)


    async def shutdown(signal_name, subscriber):
        """
        This method is responsible for shutting down the subscriber. Even When Recieved The Signal
        :param signal_name:
        :param subscriber:
        :return:
        """
        logging.info(f"Received signal {signal_name}. Closing connection.")
        if subscriber.connection.is_open:
            await subscriber.close_connection()

        tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        [task.cancel() for task in tasks]

        logging.info(f"Cancelling {len(tasks)} tasks")
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                logging.error(f"Got exception during shutdown: {result}")

        logging.info("Shutdown complete.")

        # Close the event loop
        loop = asyncio.get_event_loop()
        loop.stop()
        while not loop.is_closed():
            await asyncio.sleep(1)

    for sig in (signal.SIGINT, signal.SIGTERM):
        asyncio.get_event_loop().add_signal_handler(sig, lambda sig=sig: asyncio.create_task(shutdown(sig, subscriber)))

    try:
        asyncio.run(subscriber.run())
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
    finally:
        logging.info("Subscriber closed.")