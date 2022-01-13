import typing

from pydantic import UUID4

from api.base_models import BCM, BUM
from database.db import BDBM, MongoDatabase
from utils.logger import setup_logger

LOGGER = setup_logger()


class DBManagerException(Exception):
    pass


class ObjectAlreadyExists(DBManagerException):
    pass


class ObjectDoesntExist(DBManagerException):
    pass


class BaseDBManager(typing.Generic[BCM, BDBM]):
    object_db_model: typing.Type[BDBM]

    def __init__(self, model_db: MongoDatabase[BDBM]):
        self.model_db = model_db

    async def get(self, id: UUID4, user_id: UUID4) -> BDBM:
        """Get an object by id"""
        db_object = await self.model_db.get(id)
        LOGGER.debug(db_object)

        if db_object is None or db_object.created_by != user_id:
            raise ObjectDoesntExist()

        return db_object

    def create_to_db(self, create_object: BCM, user_id: UUID4) -> BDBM:
        """Convert BCM to BDBM"""
        raise NotImplementedError()

    async def create(self, create_object: BCM, user_id: UUID4) -> BDBM:
        """Create an object in database"""

        created_object = await self.model_db.create(
            self.create_to_db(create_object, user_id)
        )

        return created_object

    def base_filter(self, user_id: UUID4) -> dict:
        return {"owner": user_id}

    async def list(self, user_id: UUID4, **kwargs) -> list[BDBM]:
        filter_params = {**self.base_filter(user_id), **kwargs}
        print(filter_params)
        return await self.model_db.filter(filter_params)

    async def update(
        self,
        object_update: BUM,
        current_object: BDBM,
        user_id: UUID4,
        safe: bool = True,
    ) -> BDBM:
        """
        Update an object.

        # TODO: Triggers the on_after_update handler on success
        """
        if safe:
            updated_object_data = object_update.create_update_dict()
        else:
            updated_object_data = object_update.create_update_dict_superuser()
        updated_object = await self._update(current_object, updated_object_data)
        # await self.on_after_update(updated_object, updated_object_data)
        return updated_object

    async def _update(
        self, current_object: BDBM, update_dict: dict[str, typing.Any]
    ) -> BDBM:
        for field in update_dict:
            setattr(current_object, field, update_dict[field])
        return await self.model_db.update(current_object)

    async def delete(self, db_object: BDBM) -> None:
        """Delete an object"""
        await self.model_db.delete(db_object)
