import asyncio
import logging
from datetime import timezone

from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.types import PeerChannel

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Existing Message Fetching Functions ---

async def fetch_last_n_messages(user_client, channel_name, n_messages: int = 10, get_media: bool = False):
    """
    Fetch the last n messages from a Telegram channel.

    Args:
        user_client: The TelegramClient instance.
        channel_name: The username of the channel.
        n_messages: Number of messages to fetch.

    Returns:
        A list of message data dictionaries conforming to the Message schema.
    """
    logger.info(f"Fetching messages from channel: {channel_name}")
    try:
        channel = await user_client.get_entity(channel_name)
    except Exception as e:
        logger.error(f"Failed to get entity for channel {channel_name}: {e}")
        return []

    try:
        result = await user_client(GetHistoryRequest(
            peer=PeerChannel(channel.id),
            limit=n_messages,
            offset_date=None,
            offset_id=0,
            max_id=0,
            min_id=0,
            add_offset=0,
            hash=0
        ))
    except Exception as e:
        logger.error(f"Failed to fetch history for channel {channel_name}: {e}")
        return []

    messages = result.messages
    data = []
    for message in messages:
        print(message)
        # Ensure message text is not None before processing
        message_text = message.message if message.message else ""
        if get_media:
            await user_client.download_media(message.media, f"media/{message.id}/{message.media}")
        message_data = {
            "id": message.id,  # Add this line for Pydantic validation
            "post_id": message.id,  # Keep for compatibility
            "datetime": message.date.astimezone(timezone.utc),
            "text": message_text,
            "channel_name": channel_name,
            "photo": None,
            "caption": None
        }
        data.append(message_data)
    return data

async def fetch_messages(channels: list[str], user_client, n_messages: int = 10):
    tasks = [
        asyncio.create_task(
            fetch_last_n_messages(user_client, channel, n_messages),
            name=f"fetch from {channel}"
        )
        for channel in channels
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    for result in results:
        if isinstance(result, Exception):
            logger.error(f"An error occurred while fetching messages: {result}")
        else:
            logger.info(f"Fetched {len(result)} messages.")
