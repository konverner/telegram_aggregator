import os
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt
from pydantic import BaseModel, ValidationError

# --- JWT Configuration ---
# Load from environment variables with defaults
SECRET_KEY = os.getenv("SECRET_KEY", "a_very_secret_key_should_be_long_and_random") # Replace with a strong, environment-managed key
ALGORITHM = os.getenv("ALGORITHM", "HS256")
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", 420)) # Default to 7 hours

# OAuth2 scheme for extracting the token from the Authorization header
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="auth/token") # Adjust tokenUrl if your login endpoint is different

# --- Pydantic model for token data ---
class TokenData(BaseModel):
    username: Optional[str] = None

# --- Token Creation ---
def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    """Creates a JWT access token."""
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.now(timezone.utc) + expires_delta
    else:
        expire = datetime.now(timezone.utc) + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

# --- Token Verification Dependency ---
async def get_current_user(token: str = Depends(oauth2_scheme)) -> str:
    """
    Dependency function to verify the JWT token and return the username (phone number).
    Raises HTTPException if the token is invalid or expired.
    """
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        # 'sub' claim typically holds the user identifier
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
        # Optional: You could add more validation here, e.g., check against a TokenData model
        # token_data = TokenData(username=username)
    except JWTError as e:
        # Log the error for debugging if needed
        # logger.error(f"JWT Error: {e}")
        raise credentials_exception from e
    except ValidationError as e:
        # Handle potential Pydantic validation errors if using TokenData model
        # logger.error(f"Token Data Validation Error: {e}")
        raise credentials_exception from e

    # Here, username is the phone number stored in the 'sub' claim
    return username
