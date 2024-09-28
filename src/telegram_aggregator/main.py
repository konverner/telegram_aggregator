import logging.config
import os

import pandas as pd
from dotenv import find_dotenv, load_dotenv
from omegaconf import OmegaConf
from telethon import TelegramClient, events
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.types import PeerChannel

# Load logging configuration
logging_config = OmegaConf.to_container(
    OmegaConf.load("./src/telegram_aggregator/conf/logging_config.yaml"),
    resolve=True
)
logging.config.dictConfig(logging_config)

# Configure logging
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv(find_dotenv(usecwd=True))
API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")
PHONE_NUMBER = os.getenv("PHONE_NUMBER")

if not API_ID or not API_HASH or not BOT_TOKEN or not PHONE_NUMBER:
    logger.error("API_ID, API_HASH, BOT_TOKEN, or PHONE_NUMBER is not set in the environment variables.")
    exit(1)

DOWNLOAD_DIR = "input/"

# Initialize Telegram clients
bot_client = TelegramClient('bot_session', int(API_ID), API_HASH)
bot_client.start(bot_token=BOT_TOKEN)
logger.info(f"Bot `{bot_client.get_me()}` has started")

user_client = TelegramClient('user_session', int(API_ID), API_HASH)


config = OmegaConf.load("./src/telegram_aggregator/conf/config.yaml")

async def get_last_n_messages(user_client, channel_username):
    channel = await user_client.get_entity(channel_username)
    result = await user_client(GetHistoryRequest(
        peer=PeerChannel(channel.id),
        limit=config.last_n_messages,
        offset_date=None,
        offset_id=0,
        max_id=0,
        min_id=0,
        add_offset=0,
        hash=0
    ))
    messages = result.messages
    data = []
    for message in messages:
        data.append({
            'datetime': message.date.replace(tzinfo=None),
            'content': message.message,
            'id': message.id,
            'channel_name': channel_username
        })
    return data

async def fetch_all_messages(channels: list[str]) -> list[dict[str, str]]:
    """Fetch messages from the given channels"""
    all_data = []
    user_client = TelegramClient(PHONE_NUMBER, API_ID, API_HASH)
    await user_client.start()

    for channel in channels:
        all_data.extend(await get_last_n_messages(user_client, channel))
    return all_data


@bot_client.on(events.NewMessage(pattern='/start'))
async def send_welcome(event):
    """Send a welcome message when the user sends /start"""
    await event.respond("Please send a list of channel names separated by new lines.")

@bot_client.on(events.NewMessage)
async def handle_channel_list(event):
    """Handle the list of channel names sent by the user"""
    if event.message.message.startswith('/'):
        return  # Ignore commands

    channels = event.message.message.split('\n')
    channels = [channel.strip() for channel in channels if channel.strip()]

    if not channels:
        await event.reply("No valid channel names provided. Please send a list of channel names separated by new lines.")
        return

    logger.info(f"Channels to fetch: {channels}")
    await event.reply("Fetching last 10 messages from channels...")

    records = await fetch_all_messages(channels)
    df = pd.DataFrame(records)

    user_directory = f"output/{event.sender_id}"
    if not os.path.exists(user_directory):
        os.makedirs(user_directory)
    output_file = f"{user_directory}/data.xlsx"
    df.to_excel(output_file, index=False)
    await bot_client.send_file(event.sender_id, output_file, caption="Here are the collected messages.")

if __name__ == "__main__":
    bot_client.run_until_disconnected()
