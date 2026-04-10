from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.db import init_db
from app.router import router


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield


app = FastAPI(title="Risk Alert Service", lifespan=lifespan)

app.include_router(router)
