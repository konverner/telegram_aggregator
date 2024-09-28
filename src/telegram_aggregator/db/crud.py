import datetime
import logging
from sqlalchemy import select, and_

from omegaconf import OmegaConf
from .models import Message, Channel
from .database import get_session


# Load logging configuration with OmegaConf
logging_config = OmegaConf.to_container(OmegaConf.load("./src/telegram_aggregator/conf/logging_config.yaml"), resolve=True)

# Apply the logging configuration
logging.config.dictConfig(logging_config)

# Configure logging
logger = logging.getLogger(__name__)


def add_message(
        id: int,
        datetime: datetime.datetime,
        content: str,
        channel_name: str
    ) -> None:
    session = get_session()
    try:
        # Check if a record with the same primary key (message_id) already exists
        existing_message = session.query(Message).filter_by(id=id).first()

        if existing_message is None:
            # If no existing record is found, add the new message
            message = Message(
                id=id,
                datetime=datetime,
                content=content,
                channel_name=channel_name
            )
            session.add(message)
            session.commit()
            logger.info(f"Message added to the database: {message.id}")
        else:
            logger.info(f"Message with id `{id}` already exists, not adding.")
    except Exception as e:
        session.rollback()
        logger.error(f"An error occurred: {e}")
    finally:
        session.close()

def add_channel(name: str, comment: str = None) -> None:
    session = get_session()
    channel = Channel(name=name, comment=comment)
    session.add(channel)
    session.commit()
    logger.info(f"Channel added to the database: {channel}")
    session.close()

def get_channel_names() -> list[str]:
    session = get_session()
    try:
        # Query to get distinct channel names
        query = select(Channel.name).distinct()
        result = session.execute(query)

        # Fetch all distinct channel names
        channel_names = [row[0] for row in result]
        return channel_names
    finally:
        session.close()

def get_messages_in_timerange(
        start_time: datetime.datetime,
        end_time: datetime.datetime
    ) -> list[Message]:
    session = get_session()
    try:
        # Query to filter messages based on the time range
        query = select(Message).where(
            and_(Message.datetime >= start_time, Message.datetime <= end_time)
        )
        result = session.execute(query)

        # Fetch all messages in the given time range
        messages = result.scalars().all()
        return messages

    finally:
        session.close()
