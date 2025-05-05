import os
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from ..database.core import get_session
from .models import User

# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# --- Configuration ---
SECRET_KEY = os.getenv("SECRET_KEY", "your-super-secret-key") # Replace with a strong, random key
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/login") # Adjust tokenUrl if needed, though not used directly for password flow here

# --- JWT Functions ---
def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    """Creates a JWT access token."""
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.now(timezone.utc) + expires_delta
    else:
        expire = datetime.now(timezone.utc) + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    logger.info(f"Created access token for user: {data.get('sub')} with expiry: {expire}")
    return encoded_jwt

# --- Dependency ---
async def get_current_user(
    token: str = Depends(oauth2_scheme), # Token string directly from header
    db: AsyncSession = Depends(get_session)
) -> User:
    """FastAPI dependency to get the current user object by matching the provided token."""
    logger.info("Authenticating user via access token lookup")
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials", # Generic error message
        headers={"WWW-Authenticate": "Bearer"},
    )

    if not token:
        logger.warning("Authentication attempt with no token provided.")
        raise credentials_exception

    # Verify user exists in database by matching the token directly
    # Assuming the User model has an 'access_token' column where the token is stored
    query = select(User).where(User.access_token == token)
    logger.info(f"Querying database for user with provided token.") # Avoid logging the token itself
    result = await db.execute(query)
    user = result.scalar_one_or_none()

    if user is None:
        logger.warning(f"User not found for the provided token.")
        raise credentials_exception # Use the same exception for not found or invalid token

    # Optional: Check token expiry stored in the database if you have an 'expires_at' field
    # if user.expires_at and user.expires_at < datetime.now(timezone.utc):
    #     logger.warning(f"Token expired for user: {user.phone_number}")
    #     # Optionally delete the expired token from the user record or mark user inactive
    #     # await db.delete(user) # Example: or update user status
    #     # await db.commit()
    #     raise credentials_exception # Treat expired token as invalid

    # Optional: Check if user is active
    # if not user.is_active:
    #     logger.warning(f"Inactive user attempted access: {user.phone_number}")
    #     raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Inactive user")

    logger.info(f"User authenticated successfully: {user.phone_number}")
    return user # Return the full User object
