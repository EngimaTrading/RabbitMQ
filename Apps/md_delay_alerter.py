import asyncio
import aio_pika
import time
import sys
from Subscriber.subscriber import Subscriber
from LogParser.UpdateCycleInfo import UpdateCycleInfo, IsData
import logging
from slack_sender import SlackMessageSender

THRESHOLD = 0.1  # Define your threshold here in Seconds


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
        self.trade_name = trade_name
        self.subscriber = Subscriber(connection_string, queue_name, exchange_name, trade_name, 'INFO', 'UPC')
        self.rate_limiter = RateLimiter(10)  # rate limit of 60 seconds
        self.alert_queue = asyncio.Queue()

    async def process_messages(self):
        await self.subscriber.connect()
        async for message in self.subscriber.get_messages():
            split_line = message.decode('utf-8').split('|')
            if IsData(split_line):
                update_cycle_info = UpdateCycleInfo()
                update_cycle_info.ParseLine(split_line)
                first_pkt_diff = ((update_cycle_info.first_pkt_rcv_ts - update_cycle_info.first_pkt_exch_ts) / 1e9) if update_cycle_info.first_pkt_rcv_ts and update_cycle_info.first_pkt_exch_ts else None
                last_pkt_diff = ((update_cycle_info.last_pkt_rcv_ts - update_cycle_info.last_pkt_exch_ts) / 1e9) if update_cycle_info.last_pkt_rcv_ts and update_cycle_info.last_pkt_exch_ts else None
                if first_pkt_diff and last_pkt_diff:
                    if abs(first_pkt_diff) > THRESHOLD or abs(last_pkt_diff) > THRESHOLD:
                        if self.rate_limiter.should_send(message):  # pass the message directly
                            await self.alert_queue.put((message, first_pkt_diff, last_pkt_diff))

    async def get_alerts(self):
        slack_sender = SlackMessageSender(['md_delay_alert'], 'xoxb-1442253132338-6771480199300-vIyBd9AVmwq1r39FuD4M8NJX')
        while True:
            alert = await self.alert_queue.get()
            message, first_pkt_diff, last_pkt_diff = alert
            print(alert)
            formatted_message = f"```MD Delay threshold exceeded for Line:\n{message}\nFirst packet Difference: {first_pkt_diff}\nLast Packet Difference: {last_pkt_diff}```"
            # await slack_sender.send_message(formatted_message)
            logging.warning(f"Threshold exceeded. Alert: {alert}")


async def main() -> None:
    trade_name = sys.argv[1]
    md_delay_alerter = MD_Delay_Alerter(trade_name)
    task_process_messages = asyncio.create_task(md_delay_alerter.process_messages())
    task_get_alerts = asyncio.create_task(md_delay_alerter.get_alerts())
    await asyncio.gather(task_process_messages, task_get_alerts)


if __name__ == "__main__":
    asyncio.run(main())
