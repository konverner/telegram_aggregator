import asyncio
import logging
import os
import sys
from dotenv import find_dotenv, load_dotenv
from omegaconf import OmegaConf
from telegram_aggregator.db.crud import get_channel_names
from telegram_aggregator.db.database import create_tables
from telegram_aggregator.core.fetch import fetch_messages
from telethon import TelegramClient

# Load configuration
config = OmegaConf.load("./src/telegram_aggregator/conf/config.yaml")

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv(find_dotenv(usecwd=True))
API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
PHONE_NUMBER = os.getenv("PHONE_NUMBER")

# Validate essential environment variables
if not all([API_ID, API_HASH, PHONE_NUMBER]):
    logger.error("API_ID, API_HASH, and PHONE_NUMBER must be set in the environment variables.")
    exit(1)


async def main_loop():
    await create_tables()
    user_client = TelegramClient(PHONE_NUMBER, API_ID, API_HASH)
    await user_client.start(phone=PHONE_NUMBER)

    while True:
        try:
            channels = await get_channel_names()
            logger.info(f"Fetching messages from channels: {channels}")
            await fetch_messages(channels, user_client)
            logger.info("Messages fetched successfully.")
        except Exception as e:
            logger.exception(f"An error occurred while fetching messages from channels: {e}")
        break
        await asyncio.sleep(config.interval_seconds)

if __name__ == "__main__":
    import time
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    start_time = time.time()
    asyncio.run(main_loop())
    print("--- %s seconds ---" % (time.time() - start_time))