import motor.motor_asyncio

from config import settings

client = motor.motor_asyncio.AsyncIOMotorClient(
    settings.database_url, uuidRepresentation="standard"
)
db = client[settings.database_name]
first_step_collection = db["first_step_collection"]


def get_first_step_collection():
    return first_step_collection
