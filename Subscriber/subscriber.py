import asyncio
import aio_pika

async def process_message(message: aio_pika.abc.AbstractIncomingMessage,) -> None:
    async with message.process():
        print(message.body)
        await asyncio.sleep(0)


async def main() -> None:
    connection = await aio_pika.connect_robust(
        "amqp://guest:guest@127.0.0.1/",
    )

    channel = await connection.channel()
    await channel.set_qos(prefetch_count=100)

    queue = await channel.declare_queue('', auto_delete=False, durable=False, exclusive=True)
    topic_logs_exchange = await channel.declare_exchange(
        "enigma_logs", aio_pika.ExchangeType.TOPIC,
    )

    # for binding_key in binding_keys:
    binding_key = "brz_mid.*.MD"
    await queue.bind(topic_logs_exchange, routing_key=binding_key)
    await queue.consume(process_message)

    try:
        # Wait until terminate
        await asyncio.Future()
    finally:
        await connection.close()


if __name__ == "__main__":
    asyncio.run(main())