import asyncio
import logging
import os
import sys
import argparse  # Import argparse

from dotenv import find_dotenv, load_dotenv
from omegaconf import OmegaConf
from telethon import TelegramClient

from ..src.app.messages.service import fetch_messages

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

# Set up argument parser
parser = argparse.ArgumentParser(description="Fetch messages from specified Telegram channels.")
parser.add_argument('--channels', type=str, help='Comma-separated list of channel names to fetch from.')

args = parser.parse_args()


async def main_loop():
    user_client = TelegramClient(PHONE_NUMBER, API_ID, API_HASH)
    await user_client.start(phone=PHONE_NUMBER)

    while True:
        try:
            # Use channels from command line arguments if provided
            channels = [channel.strip() for channel in args.channels.split(',')]
            n_messages = args.n_messages if args.n_messages else None
            logger.info(f"Fetching messages from command-line specified channels: {channels}")

            if channels: # Only proceed if there are channels to fetch
                await fetch_messages(channels, user_client, n_messages)
                logger.info("Messages fetched successfully.")
            else:
                logger.info("No channels specified or found in the database. Skipping fetch.")

        except Exception as e:
            logger.exception(f"An error occurred while fetching messages: {e}")

if __name__ == "__main__":
    import time
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    start_time = time.time()
    try:
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        logger.info("Script interrupted by user.")
    finally:
        print("--- Script finished ---")
        print("--- Total runtime: %s seconds ---" % (time.time() - start_time))
