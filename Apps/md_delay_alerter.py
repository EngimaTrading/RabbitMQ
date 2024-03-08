import asyncio
import aio_pika
import time
import sys
from Subscriber.subscriber import Subscriber
from LogParser.UpdateCycleInfo import UpdateCycleInfo, IsData
import logging

THRESHOLD = 0.000001  # Define your threshold here in Seconds


class RateLimiter:
    def __init__(self, rate_limit):
        self.rate_limit = rate_limit
        self.messages = {}

    def should_send(self, message):
        now = time.time()
        if message in self.messages:
            last_sent = self.messages[message]
            if now - last_sent < self.rate_limit:
                return False
        self.messages[message] = now
        return True


class MD_Delay_Alerter:
    def __init__(self, trade_name):
        connection_string = "amqp://guest:guest@127.0.0.1/"
        queue_name = ''
        exchange_name = "enigma_logs"
        self.subscriber = Subscriber(connection_string, queue_name, exchange_name, trade_name, 'INFO', 'UPC')
        self.rate_limiter = RateLimiter(20)  # rate limit of 60 seconds
        self.alert_queue = asyncio.Queue()

    async def process_messages(self):
        await self.subscriber.connect()
        async for message in self.subscriber.get_messages():
            message_str = message.decode('utf-8')
            split_line = message_str.split('|')
            if IsData(split_line):
                update_cycle_info = UpdateCycleInfo()
                update_cycle_info.ParseLine(split_line)
                # Also Check if None Of The Values are Infinity or Zero OtherWise Ignore
                first_pkt_diff = update_cycle_info.first_pkt_rcv_ts - update_cycle_info.first_pkt_exch_ts / 1e9 if update_cycle_info.first_pkt_rcv_ts and update_cycle_info.first_pkt_exch_ts else None
                last_pkt_diff = update_cycle_info.last_pkt_rcv_ts - update_cycle_info.last_pkt_exch_ts / 1e9 if update_cycle_info.last_pkt_rcv_ts and update_cycle_info.last_pkt_exch_ts else None
                if first_pkt_diff and last_pkt_diff:
                    if abs(first_pkt_diff) > THRESHOLD or abs(last_pkt_diff) > THRESHOLD:
                        if self.rate_limiter.should_send(message):  # pass the message directly
                            await self.alert_queue.put(message)

    async def get_alerts(self):
        while True:
            alert = await self.alert_queue.get()
            print(alert)
            logging.warning(f"Threshold exceeded. Alert: {alert}")


async def main() -> None:
    trade_name = sys.argv[1]
    md_delay_alerter = MD_Delay_Alerter(trade_name)
    task_process_messages = asyncio.create_task(md_delay_alerter.process_messages())
    task_get_alerts = asyncio.create_task(md_delay_alerter.get_alerts())
    await asyncio.gather(task_process_messages, task_get_alerts)


if __name__ == "__main__":
    asyncio.run(main())
