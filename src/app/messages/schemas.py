from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime # Import datetime


class MessageRequest(BaseModel):
    channels: List[str]
    n_messages: int
    keywords: Optional[List[str]] = None

# Define the new Message schema
class Message(BaseModel):
    id: int
    datetime: datetime # Use datetime type
    channel_name: str
    text: Optional[str] = None
    photo: Optional[str] = None # Assuming image content is a URL/path or similar string representation
    caption: Optional[str] = None

    class Config:
        orm_mode = True # Enable ORM mode for compatibility with SQLAlchemy models

# Update MessageResponse to return a list of Messages
class MessagesResponse(BaseModel):
    messages: List[Message]

class ErrorResponse(BaseModel):
    detail: str
