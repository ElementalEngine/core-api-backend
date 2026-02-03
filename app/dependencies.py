from fastapi import Request
from motor.motor_asyncio import AsyncIOMotorClient

def get_database(request: Request) -> AsyncIOMotorClient:
    client = getattr(request.app.state, "mongodb_client", None)
    if client is None:
        raise RuntimeError("Mongo client not initialized")
    return client