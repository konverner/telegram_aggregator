import logging
from datetime import datetime, timedelta

from telethon import TelegramClient 
from telethon.errors import SessionPasswordNeededError
from sqlalchemy.ext.asyncio import AsyncSession

from .auth_utils import create_access_token
from .models import User

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# In-memory store for phone_code_hash (Not suitable for production)
auth_state = {}

# --- Authentication Service Functions ---

async def start_authentication(phone_number: str, api_id: str, api_hash: str):
    """Initiates Telegram authentication and sends the code."""
    client = TelegramClient(phone_number, api_id, api_hash)
    try:
        await client.connect()
        if not await client.is_user_authorized():
            sent_code = await client.send_code_request(phone_number)
            auth_state[phone_number] = sent_code.phone_code_hash  # Store hash
            await client.disconnect()
            logger.info(f"Code sent to {phone_number}")
            return True, "Verification code sent."
        else:
            await client.disconnect()
            logger.info(f"User {phone_number} is already authorized.")
            # Consider how to handle already authorized users.
            # For now, just indicate success but maybe no code was sent.
            return True, "User already authorized."
    except Exception as e:
        logger.error(f"Authentication initiation failed for {phone_number}: {e}")
        if client.is_connected():
            await client.disconnect()
        return False, f"Failed to send code: {e}"

async def save_user_to_db(
    session: AsyncSession, 
    phone_number: str, 
    access_token: str,
    expires_at: datetime
) -> User:
    """Save or update user in database."""
    user = await session.get(User, phone_number)
    if not user:
        user = User(
            phone_number=phone_number,
            access_token=access_token,
            expires_at=expires_at,
            is_active=1
        )
        session.add(user)
    else:
        user.access_token = access_token
        user.expires_at = expires_at
        user.is_active = 1
    await session.commit()
    return user

async def complete_authentication(phone_number: str, code: str, api_id: str, api_hash: str, db: AsyncSession):
    """Completes Telegram authentication and returns an access token."""
    client = TelegramClient(phone_number, api_id, api_hash)
    phone_code_hash = auth_state.get(phone_number)
    if not phone_code_hash:
        # Return None for token on failure
        return False, "Authentication process not initiated or hash expired.", None

    try:
        await client.connect()
        # Check if 2FA is needed before signing in fully
        try:
            await client.sign_in(phone_number, code=code, phone_code_hash=phone_code_hash)
        except SessionPasswordNeededError:
            # Handle 2FA if needed - this example does not cover it fully yet
            # For now, treat as failure requiring password step (not implemented)
            await client.disconnect()
            logger.warning(f"Two-factor authentication required for {phone_number}.")
            if phone_number in auth_state:
                del auth_state[phone_number] # Clean up state
            return False, "Two-factor authentication required.", None

        # Clean up stored hash after successful login
        if phone_number in auth_state:
            del auth_state[phone_number]
        await client.disconnect()
        logger.info(f"Successfully authenticated {phone_number}")

        # --- Generate JWT Token ---
        # Expiry is handled within create_access_token using ACCESS_TOKEN_EXPIRE_MINUTES from auth_utils
        access_token = create_access_token(
            data={"sub": phone_number} # Use 'sub' claim for user identifier (phone number)
        )
        
        # Save user to database
        expires_at = datetime.utcnow() + timedelta(days=30)  # Set appropriate expiration
        await save_user_to_db(db, phone_number, access_token, expires_at)
        
        # Return token along with success message
        return True, "Authentication successful.", access_token

    except Exception as e:
        logger.error(f"Authentication completion failed for {phone_number}: {e}")
        if client.is_connected():
            await client.disconnect()
        # Clean up state on other errors too
        if phone_number in auth_state:
            del auth_state[phone_number]
        # Return None for token on failure
        return False, f"Failed to login: {e}", None
