from pydantic import BaseModel
from typing import List, Optional

class MessageRequest(BaseModel):
    channels: List[str]
    n_messages: int
    keywords: Optional[List[str]] = None

class MessageResponse(BaseModel):
    message_id: int
    message_datetime: str
    content: str
    channel_name: str

class ErrorResponse(BaseModel):
    detail: str
