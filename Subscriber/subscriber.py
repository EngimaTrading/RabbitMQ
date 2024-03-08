import asyncio
import aio_pika
from aio_pika import connect, ExchangeType, exceptions

class Subscriber:
    def __init__(self, connection_string, queue_name, exchange_name, trade_name, log_type, msg_type):
        self.connection_string = connection_string
        self.queue_name = queue_name
        self.exchange_name = exchange_name
        self.binding_key = f"{trade_name}.{log_type}.{msg_type}"
        self.connection = None
        self.channel = None
        self.queue = None

    async def connect(self):
        while True:
            try:
                self.connection = await connect(self.connection_string)
                self.channel = await self.connection.channel()
                await self.channel.set_qos(prefetch_count=100)
                self.queue = await self.channel.declare_queue(self.queue_name, auto_delete=False, durable=False, exclusive=True)
                topic_logs_exchange = await self.channel.declare_exchange(self.exchange_name, ExchangeType.TOPIC)
                await self.queue.bind(topic_logs_exchange, routing_key=self.binding_key)
                break
            except exceptions.AMQPError as e:
                print(f"Connection failed. Retrying in 5 seconds... Error: {e}")
                await asyncio.sleep(5)

    async def get_messages(self):
        async with self.queue.iterator() as queue_iter:
            async for message in queue_iter:
                yield message.body

    async def close_connection(self) -> None:
        """
        This method is responsible for closing the connection to the RabbitMQ server.
        :return:
        """
        if self.channel is not None:
            await self.channel.close()
        if self.connection is not None:
            await self.connection.close()


# async def process_message(message: aio_pika.abc.AbstractIncomingMessage, ) -> None:
#     async with message.process():
#         print(message.body)
#         await asyncio.sleep(0)
#
#
# async def main() -> None:
#     connection = await aio_pika.connect_robust(
#         "amqp://guest:guest@127.0.0.1/",
#     )
#
#     channel = await connection.channel()
#     await channel.set_qos(prefetch_count=100)
#
#     queue = await channel.declare_queue('', auto_delete=False, durable=False, exclusive=True)
#     topic_logs_exchange = await channel.declare_exchange(
#         "enigma_logs", aio_pika.ExchangeType.TOPIC,
#     )
#
#     # for binding_key in binding_keys:
#     binding_key = "brz_mid2.*.MD"
#     await queue.bind(topic_logs_exchange, routing_key=binding_key)
#     await queue.consume(process_message)
#
#     try:
#         # Wait until terminate
#         await asyncio.Future()
#     finally:
#         await connection.close()


# if __name__ == "__main__":
#     asyncio.run(main())
