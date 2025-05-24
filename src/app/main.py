import asyncio
import sys
import logging
import uvicorn
from fastapi import FastAPI, Request
import os
from .database.core import create_tables
from .messages.router import router as messages_router
from .auth.router import router as auth_router
from telethon import TelegramClient

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
PHONE_NUMBER = os.getenv("PHONE_NUMBER")


if not all([API_ID, API_HASH, PHONE_NUMBER]):
    logger.critical("CRITICAL: PHONE_NUMBER, API_ID and API_HASH must be set in the environment variables.")
    raise Exception("PHONE_NUMBER, API_ID and API_HASH must be set in the environment variables.")


app = FastAPI()

@app.on_event("startup")
async def startup_event():
    logger.info("Application startup: Initializing database and Telegram client...")
    await create_tables()
    user_client = TelegramClient(PHONE_NUMBER, API_ID, API_HASH)
    try:
        logger.info(f"Attempting to connect to Telegram as {PHONE_NUMBER}...")
        await user_client.start(phone=PHONE_NUMBER)
        if await user_client.is_user_authorized():
            logger.info("Telegram client connected and authorized successfully.")
            app.state.telegram_client = user_client
        else:
            logger.error("Telegram client connected but not authorized. Please check credentials or complete authorization.")
            app.state.telegram_client = user_client 
    except Exception as e:
        logger.error(f"Failed to initialize or start Telegram client: {e}")
        app.state.telegram_client = None

@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Application shutdown: Disconnecting Telegram client...")
    if hasattr(app.state, 'telegram_client') and app.state.telegram_client:
        if app.state.telegram_client.is_connected():
            await app.state.telegram_client.disconnect()
            logger.info("Telegram client disconnected.")

app.include_router(messages_router, tags=["messages"])
app.include_router(auth_router, tags=["auth"])

if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    uvicorn.run(app, host="0.0.0.0", port=8100)
