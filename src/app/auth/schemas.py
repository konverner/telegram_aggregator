
from pydantic import BaseModel
from typing import Optional

# --- Authentication Schemas ---

class PhoneRequest(BaseModel):
    phone_number: str

class CodeRequest(BaseModel):
    phone_number: str
    code: str

class AuthResponse(BaseModel):
    message: str
    success: bool
    access_token: Optional[str] = None # Add access token field
    token_type: Optional[str] = None # Standard practice to include token type
