from sqlalchemy import Column, DateTime, ForeignKey, Integer, String, Text

from ..models import Base, TimestampMixin


class Channel(Base, TimestampMixin):
    __tablename__ = "channels"

    id = Column(Integer, autoincrement=True)
    name = Column(String, primary_key=True)
    comment = Column(String)


class Message(Base, TimestampMixin):
    __tablename__ = 'messages'

    id = Column(Integer, primary_key=True)
    datetime = Column(DateTime)
    text = Column(Text, nullable=True)
    photo = Column(String, nullable=True)
    caption = Column(String, nullable=True)
    channel_name = Column(String, ForeignKey('channels.name'))