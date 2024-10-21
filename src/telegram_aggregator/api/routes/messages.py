import os
import logging
from fastapi import APIRouter, HTTPException
from telegram_aggregator.api.schemas import ErrorResponse, MessageRequest, MessageResponse
from telegram_aggregator.core.fetch import fetch_last_n_messages
from telethon import TelegramClient

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

router = APIRouter()

API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
PHONE_NUMBER = os.getenv("PHONE_NUMBER")

if not all([API_ID, API_HASH, PHONE_NUMBER]):
    raise Exception("API_ID, API_HASH, and PHONE_NUMBER must be set in the environment variables.")

@router.post("/messages", response_model=list[MessageResponse], responses={400: {"model": ErrorResponse}})
async def get_messages(request: MessageRequest):
    if not all([API_ID, API_HASH, PHONE_NUMBER]):
        raise HTTPException(status_code=400, detail="API_ID, API_HASH, and PHONE_NUMBER must be set in the environment variables.")
    logger.info(f"Fetching messages from channels: {request.channels}")
    user_client = TelegramClient(PHONE_NUMBER, API_ID, API_HASH)
    await user_client.start(phone=PHONE_NUMBER)

    all_messages = []
    for channel in request.channels:
        messages = await fetch_last_n_messages(user_client, channel, request.n_messages)
        # datetime to string
        for msg in messages:
            msg['message_datetime'] = msg['message_datetime'].strftime("%Y-%m-%d %H:%M:%S")
        if request.keywords:
            messages = [msg for msg in messages if any(keyword in msg['content'] for keyword in request.keywords)]
        all_messages.extend(messages)

    await user_client.disconnect()
    return all_messages
