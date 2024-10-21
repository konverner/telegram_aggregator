import datetime
import logging
from typing import Optional

from sqlalchemy import and_, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.future import select

from .database import get_session
from .models import Channel, Message

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def add_message(
        message_id: int,
        message_datetime: datetime.datetime,
        content: str,
        channel_name: str
    ) -> None:
    """
    Add a new message to the database if it doesn't already exist.

    Args:
        message_id (int): The unique identifier of the message.
        message_datetime (datetime.datetime): The timestamp of the message.
        content (str): The content of the message.
        channel_name (str): The name of the channel.
    """
    session_maker = get_session()
    async with session_maker() as session:
        async with session.begin():
            stmt = select(Message).where(Message.id == message_id)
            result = await session.execute(stmt)
            existing_message: Optional[Message] = result.scalar_one_or_none()

            if existing_message is None:
                message = Message(
                    id=message_id,
                    datetime=message_datetime,
                    content=content,
                    channel_name=channel_name
                )
                session.add(message)
                logger.info(f"Message added to the database: {message.id}")
            else:
                logger.info(f"Message with id `{message_id}` already exists, not adding.")

async def add_channel(name: str, comment: Optional[str] = None) -> None:
    """
    Add a new channel to the database if it doesn't already exist.

    Args:
        name (str): The name of the channel.
        comment (Optional[str]): Optional comment about the channel.
    """
    session_maker = get_session()
    async with session_maker() as session:
        try:
            async with session.begin():
                stmt = select(Channel).where(Channel.name == name)
                result = await session.execute(stmt)
                existing_channel: Optional[Channel] = result.scalar_one_or_none()

                if existing_channel is None:
                    channel = Channel(
                        name=name,
                        comment=comment
                    )
                    session.add(channel)
                    logger.info(f"Channel added to the database: {channel.name}")
                else:
                    logger.info(f"Channel with name `{name}` already exists, not adding.")
        except SQLAlchemyError as e:
            logger.error(f"Failed to add channel `{name}`: {e}")
            raise

async def get_channel_names() -> list[str]:
    """
    Retrieve all channel names from the database.

    Returns:
        A list of channel names.
    """
    session_maker = get_session()
    async with session_maker() as session:
        try:
            async with session.begin():
                stmt = select(Channel.name)
                result = await session.execute(stmt)
                channels = result.scalars().all()
                return channels
        except SQLAlchemyError as e:
            logger.error(f"Failed to retrieve channel names: {e}")
            return []

async def get_messages_in_timerange(
        start_time: datetime.datetime,
        end_time: datetime.datetime
    ) -> list[Message]:
    """
    Retrieve all messages within a specified time range.

    Args:
        start_time (datetime.datetime): The start of the time range.
        end_time (datetime.datetime): The end of the time range.

    Returns:
        list[Message]: A list of messages within the time range.
    """
    session_maker = get_session()
    async with session_maker() as session:
        try:
            async with session.begin():
                stmt = select(Message).where(
                    and_(
                        Message.datetime >= start_time,
                        Message.datetime <= end_time
                    )
                )
                result = await session.execute(stmt)
                messages = result.scalars().all()
                return messages
        except SQLAlchemyError as e:
            logger.error(f"Failed to retrieve messages in timerange: {e}")
            return []
