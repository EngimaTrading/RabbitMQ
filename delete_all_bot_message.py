from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

def delete_bot_messages(token, channel):
    client = WebClient(token=token)

    try:
        # Fetch message history in the channel
        response = client.conversations_history(channel=channel)
        messages = response["messages"]
    except SlackApiError as e:
        print(f"Error retrieving channel history: {e.response['error']}")
        return False

    # Identify and delete messages sent by the bot
    for message in messages:
        if "bot_id" in message and message["bot_id"] == client.auth_test().data["bot_id"]:
            try:
                # Delete the identified message
                client.chat_delete(channel=channel, ts=message["ts"])
                print(f"Message deleted successfully: Channel {channel}, Timestamp {message['ts']}")
            except SlackApiError as e:
                print(f"Error deleting message: {e.response['error']}")

    return True

# Example usage
def main():
    token = "xoxb-1442253132338-6771480199300-vIyBd9AVmwq1r39FuD4M8NJX"
    channel = "C06NE3FKNCE"

    delete_bot_messages(token, channel)

if __name__ == "__main__":
    main()
