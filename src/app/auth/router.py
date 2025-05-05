import logging
import os

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from .schemas import AuthResponse, CodeRequest, PhoneRequest
from ..messages.schemas import ErrorResponse
from .service import complete_authentication, start_authentication
from ..database.core import get_session

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

router = APIRouter(prefix="/auth")

API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")

if not all([API_ID, API_HASH]):
    raise Exception("API_ID and API_HASH must be set in the environment variables.")

@router.post("/send_code", response_model=AuthResponse, responses={400: {"model": ErrorResponse}})
async def send_code(request: PhoneRequest):
    """Initiates the login process by sending a code to the user's phone."""
    if not all([API_ID, API_HASH]):
         raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Server configuration error: API_ID or API_HASH not set.")

    logger.info(f"Initiating authentication for {request.phone_number}")
    success, message = await start_authentication(request.phone_number, API_ID, API_HASH)
    if not success:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=message)
    return AuthResponse(message=message, success=True)

@router.post("/login", response_model=AuthResponse, responses={400: {"model": ErrorResponse}})
async def login(request: CodeRequest, db: AsyncSession = Depends(get_session)):
    """Completes the login process and returns an access token."""
    if not all([API_ID, API_HASH]):
         raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Server configuration error: API_ID or API_HASH not set.")

    logger.info(f"Attempting to complete authentication for {request.phone_number}")
    success, message, access_token = await complete_authentication(
        request.phone_number, 
        request.code, 
        API_ID, 
        API_HASH,
        db
    )
    if not success:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=message)
    
    return AuthResponse(message=message, success=True, access_token=access_token, token_type="bearer")
