import asyncio
import logging
from datetime import timezone

from omegaconf import OmegaConf
from telegram_aggregator.db.crud import add_message
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.types import PeerChannel

# Load configuration
config = OmegaConf.load("./src/telegram_aggregator/conf/config.yaml")

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def fetch_last_n_messages(user_client, channel_name, n_messages: int = 10):
    """
    Fetch the last n messages from a Telegram channel.

    Args:
        user_client: The TelegramClient instance.
        channel_name: The username of the channel.
        n_messages: Number of messages to fetch.

    Returns:
        A list of message data dictionaries.
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
        message_data = {
            "message_id": message.id,
            "message_datetime": message.date.astimezone(timezone.utc),
            "content": message.message,
            "channel_name": channel_name
        }
        logger.info(f"Adding message to the database: {message.id}")
        data.extend([message_data])
        await add_message(**message_data)
    return data

async def fetch_messages(channels: list[str], user_client):
    tasks = [
        asyncio.create_task(
            fetch_last_n_messages(user_client, channel, config.last_n_messages),
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
