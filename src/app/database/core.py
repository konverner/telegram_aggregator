import asyncio
import logging.config
import os

from dotenv import find_dotenv, load_dotenv
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from ..models import Base

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv(find_dotenv(usecwd=True))

# Retrieve environment variables
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
SSL_MODE = os.getenv("SSL_MODE")
DATABASE_URL = None

# Check if any of the required environment variables are not set
if not all([DB_HOST, DB_NAME, DB_USER, DB_PASSWORD]):
    logger.warning("One or more postgresql database environment variables are not set. Using SQLite instead.")
    DATABASE_URL = "sqlite+aiosqlite:///local_database.db"
else:
    logger.info(f"Using PostgreSQL database {DB_HOST}.")
    DATABASE_URL = f"postgresql+psycopg://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?sslmode=require"


# Create the async engine
engine = create_async_engine(
    DATABASE_URL
)

# Create a configured "Session" class
AsyncSessionLocal = sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False
)

async def get_session():
    """Provide a transactional scope around a series of operations."""
    async_session = AsyncSessionLocal()
    try:
        yield async_session
    finally:
        await async_session.close()

async def create_tables():
    """Create database tables based on the metadata."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("Tables created successfully.")
