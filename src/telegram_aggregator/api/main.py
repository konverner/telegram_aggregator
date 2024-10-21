import asyncio
import sys

from fastapi import FastAPI
from telegram_aggregator.api.routes import messages

app = FastAPI()

app.include_router(messages.router, prefix="/api", tags=["messages"])

if __name__ == "__main__":
    import uvicorn
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    uvicorn.run(app, host="0.0.0.0", port=8000)
