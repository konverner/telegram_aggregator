from sqlalchemy import Column, DateTime, ForeignKey, Integer, String, Text

from ..models import Base

class User(Base):
    __tablename__ = "users"

    id = Column(Integer, autoincrement=True)
    phone_number = Column(String, primary_key=True)
    access_token = Column(String, nullable=False)
    expires_at = Column(DateTime, nullable=False)  # Store expiration time
    is_active = Column(Integer, default=1)  # 1 for active, 0 for inactive
