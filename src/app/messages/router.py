import logging

from fastapi import APIRouter, HTTPException, status, Request

from .schemas import ErrorResponse, MessageRequest, MessagesResponse
from .service import fetch_last_n_messages

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

router = APIRouter(prefix="/messages")

    
@router.post("/",
             response_model=MessagesResponse,
             responses={
                 400: {"model": ErrorResponse},
                 401: {"model": ErrorResponse},
                 403: {"model": ErrorResponse},
                 500: {"model": ErrorResponse},
                 503: {"model": ErrorResponse}
             })
async def get_messages(message_request: MessageRequest, fastapi_request: Request):
    """
    Fetches messages for the authenticated user identified by the JWT token.
    The user object is obtained from the token via the `get_current_user` dependency.
    """
    user_client = fastapi_request.app.state.telegram_client

    if not user_client or not user_client.is_connected():
        logger.error("Telegram client not available or not connected.")
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Telegram client not available or not connected.")

    # logger.info(f"Fetching messages for user {current_user.phone_number} from channels: {request.channels}")

    all_messages = []
    try:
        for channel in message_request.channels:
            messages_data = await fetch_last_n_messages(user_client, channel, message_request.n_messages, message_request.get_media)
            if message_request.keywords:
                filtered_messages = []
                for msg in messages_data:
                    msg_text = msg.get('text') or msg.get('caption') or ""
                    if any(keyword.lower() in msg_text.lower() for keyword in message_request.keywords):
                        filtered_messages.append(msg)
                all_messages.extend(filtered_messages) # Use extend to add to the list
            else:
                all_messages.extend(messages_data) # Use extend to add to the list

    except ConnectionError as e:
         # This specific ConnectionError might be less relevant now if the client is managed centrally
         # and checked at the beginning of the request. However, network issues can still occur mid-operation.
         logger.error(f"Telegram connection error during message fetching: {e}")
         raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=f"Failed to connect to Telegram during operation: {e}")
    except Exception as e:
         logger.error(f"Error fetching messages: {e}")
         # Consider if disconnecting the client here is appropriate, as it's managed by app lifecycle.
         # If an error occurs that invalidates the client session, it might need re-initialization,
         # which is beyond the scope of a single request handler.
         # if user_client.is_connected():
         #     await user_client.disconnect() # This might be too aggressive.
         raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error fetching messages: {e}")

    return {"messages": all_messages}
