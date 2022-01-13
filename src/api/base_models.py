import typing

from pydantic import BaseModel


class BaseApiModel(BaseModel):
    def create_update_dict(self):
        return self.dict(
            exclude_unset=True,
            exclude={
                "id",
                "created_by",
            },
        )


class BaseCreateModel(BaseApiModel):
    pass


class BaseUpdateModel(BaseApiModel):
    pass


BCM = typing.TypeVar("BCM", bound=BaseCreateModel)
BUM = typing.TypeVar("BUM", bound=BaseUpdateModel)
