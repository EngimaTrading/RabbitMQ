
def parse_message_type(message: str) -> str:
    """
    This function takes a message as input and returns the message type.
    :param message: The message to parse.
    :return: The message type.
    """
    message_types = ['MD', 'UPC']  # Add more message types as needed

    for msg_type in message_types:
        if f'|{msg_type}|' in message:
            return msg_type

    return 'NONE'