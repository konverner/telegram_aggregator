from sqlalchemy import Column, DateTime, ForeignKey, Integer, String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Channel(Base):
    __tablename__ = "channels"

    id = Column(Integer, autoincrement=True)
    name = Column(String, primary_key=True)
    comment = Column(String)


class Message(Base):
    __tablename__ = 'messages'

    id = Column(Integer, primary_key=True)
    datetime = Column(DateTime)
    content = Column(String)
    channel_name = Column(String, ForeignKey('channels.name'))
