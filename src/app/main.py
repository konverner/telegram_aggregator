import asyncio
import sys

import uvicorn
from fastapi import FastAPI

from .database.core import create_tables
from .messages.router import router as messages_router
from .auth.router import router as auth_router

app = FastAPI()

app.include_router(messages_router, tags=["messages"])
app.include_router(auth_router, tags=["auth"])

async def setup():
    await create_tables()

if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(setup())
    uvicorn.run(app, host="127.0.0.1", port=8000)
