import os
from datetime import datetime, timedelta
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from slack_sdk.web.async_client import AsyncWebClient
import asyncio


class SlackMessageSender:
    def __init__(self, channels, token):
        self.channels = channels
        self.client = AsyncWebClient(token=token)

    async def send_message_to_channel(self, channel, message):
        attempt = 0
        while attempt <= 3:
            attempt += 1
            try:
                response = await self.client.chat_postMessage(channel=channel, text=message)
            except SlackApiError as e:
                print(
                    f"Sleeping ... Got an error for channel {channel}: OK {e.response['ok']} | ERROR {e.response['error']}")
                await asyncio.sleep(3)
                continue
            return True
        return False

    async def send_message(self, message):
        success = True
        for channel in self.channels:
            success = success and await self.send_message_to_channel(channel, message)
        return success
