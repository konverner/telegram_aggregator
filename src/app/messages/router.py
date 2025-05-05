import logging
import os

from fastapi import APIRouter, HTTPException, status
from telethon import TelegramClient

from .schemas import ErrorResponse, MessageRequest, MessagesResponse
from .service import fetch_last_n_messages

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

router = APIRouter(prefix="/messages")

API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
PHONE_NUMBER = os.getenv("PHONE_NUMBER")

if not all([API_ID, API_HASH, PHONE_NUMBER]):
    logger.critical("CRITICAL: PHONE_NUMBER, API_ID and API_HASH must be set in the environment variables.")
    raise Exception("PHONE_NUMBER, API_ID and API_HASH must be set in the environment variables.")

@router.post("/",
             response_model=MessagesResponse,
             responses={
                 400: {"model": ErrorResponse},
                 401: {"model": ErrorResponse},
                 403: {"model": ErrorResponse},
                 500: {"model": ErrorResponse},
                 503: {"model": ErrorResponse}
             })
async def get_messages(request: MessageRequest):
    """
    Fetches messages for the authenticated user identified by the JWT token.
    The user object is obtained from the token via the `get_current_user` dependency.
    """
    if not all([API_ID, API_HASH]):
        logger.error("Server configuration error: API_ID or API_HASH not set during request.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Server configuration error.")

    # logger.info(f"Fetching messages for user {current_user.phone_number} from channels: {request.channels}")

    # print(current_user.phone_number)
    user_client = TelegramClient(PHONE_NUMBER, API_ID, API_HASH)
    await user_client.start(phone=PHONE_NUMBER)
    
    all_messages = []
    try:
        for channel in request.channels:
            messages_data = await fetch_last_n_messages(user_client, channel, request.n_messages)
            if request.keywords:
                filtered_messages = []
                for msg in messages_data:
                    msg_text = msg.get('text') or msg.get('caption') or ""
                    if any(keyword.lower() in msg_text.lower() for keyword in request.keywords):
                        filtered_messages.append(msg)
                all_messages = filtered_messages
            else:
                all_messages = messages_data

    except ConnectionError as e:
         logger.error(f"Telegram connection error for user {PHONE_NUMBER}: {e}")
         raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=f"Failed to connect to Telegram: {e}")
    except Exception as e:
         logger.error(f"Error fetching messages for user {PHONE_NUMBER}: {e}")
         if user_client.is_connected():
             await user_client.disconnect()
         raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error fetching messages: {e}")

    return {"messages": all_messages}
