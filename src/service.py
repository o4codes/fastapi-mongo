from typing import List, Optional, Type, Union

from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorDatabase
from pydantic import BaseModel

from .exceptions import BadRequest, NotFoundException
from .models import DbModel
from .repository import BaseRepository


class BaseService:
    repository_klass: Type[BaseRepository]
    data_request_klass: Type[BaseModel]
    data_response_klass: Type[BaseModel]
    model_klass: Type[DbModel]
    unique_fields: list[str]

    def __init__(self, database: AsyncIOMotorDatabase):
        self.database = database
        self.repository = self.repository_klass(database)

    async def list(
        self, size: Optional[int], page: Optional[int], **filter_kwargs
    ) -> tuple[int, list[BaseModel]]:
        """List all entities

        Args:
            size [int]: optional size used to paginate results from database
            page [int]: optional page used to denote page number to limit results from database
            filter_kwargs [dict]: keyword values used to search for entities on database

        Returns:
            tuple[int, list[BaseModel]: tuple of total results and paginated count
        """
        total_count, db_results = await self.repository.list(
            size=size, page=page, **filter_kwargs
        )
        responses = [self.data_response_klass(**result.dict()) for result in db_results]
        return total_count, responses

    async def get(self, id_: ObjectId) -> BaseModel:
        """Get a particular entity

        Args:
            id_ [ObjectId]: primary key of entity

        Returns:
            BaseModel: pydantic object of entity from database

        Raises:
            NotFoundException: when no entity with primary key is found
        """
        db_result = await self.repository.get(id_)
        if db_result is None:
            raise NotFoundException(f"Object with id {id} does not exist")
        response = self.data_response_klass(**db_result.dict())
        return response

    async def search(
        self, many: bool = False, **filter_kwargs
    ) -> Union[BaseModel, List[BaseModel]]:
        """Search for a paticular entity in ther database

        Args:
            many[bool]: boolean flag to denote if multiple results or first result is returned
            filter_kwargs[dict]: keyword values of fields and values to searh for

        Returns:
            Union[BaseModel, List[BaseModel]: entity or multiple entities found

        Raises:
            NotFoundException: when no result is found
        """
        db_result = await self.repository.search(many=many, **filter_kwargs)
        if db_result is None:
            raise NotFoundException(f"Objects matching filters not found")
        response = (
            self.data_response_klass(**db_result.dict())
            if not many
            else [self.data_response_klass(**result.dict()) for result in db_result]
        )
        return response

    async def count(self, **filter_kwargs) -> int:
        """Gets a count of entities in database

        Args:
            filter_kwargs[dict]: keyword values of fields and values to search from

        Returns:
            int: count of all entities found

        """
        return await self.repository.count(**filter_kwargs)

    async def create(self, request_instance: BaseModel) -> BaseModel:
        """Creates entity into database

        Args:
            request_instance[BaseModel]: pydantic object of entity data to insert into database

        Returns:
            BaseModel: pydantic object of data inserted

        Raises:
            BadRequest: When unique data already exists
        """
        db_model_instance = self.model_klass(**request_instance.dict())
        if self.unique_fields:
            filter_kwargs = {
                key: getattr(request_instance, key)
                for key in self.unique_fields
                if hasattr(request_instance, key) and key is not None
            }
            if await self.repository.search(**filter_kwargs):
                raise BadRequest(
                    "Cannot create data contains already exsiting unique properties"
                )
        db_result = await self.repository.create(db_model_instance)
        response = self.data_response_klass(**db_result.dict())
        return response

    async def update(self, id_: ObjectId, update_instance: BaseModel) -> BaseModel:
        """Updates entity data in database

        Args:
            id_[ObjectId]: primary key of entity to be updated
            update_instance [BaseModel]: update data

        Returns:
            BaseModel: updated entity

        Raises:
            BadRequest: when unique data already exists in database
        """
        if not await self.repository.get(id_):
            raise NotFoundException(f"Object with id {id_} is not found")
        if self.unique_fields:
            filter_kwargs = {
                key: getattr(update_instance, key)
                for key in self.unique_fields
                if hasattr(update_instance, key)
            }
            searched_object = await self.repository.search(**filter_kwargs)
            if not (searched_object and searched_object.id == id_):
                raise BadRequest(
                    "Cannot update data due to exisiting unique properties"
                )
        update_instance = self.model_klass(**update_instance.dict(exclude_unset=True))
        db_result = await self.repository.update(id_, update_instance)
        response = self.data_response_klass(**db_result.dict())
        return response

    async def delete(self, id_: ObjectId):
        """Deletes data from database

        Args:
            id_[ObjectId]: primary key of entity to be removed
        """
        if not await self.repository.get(id_):
            raise NotFoundException(f"Object with id {id_} is not found")
        await self.delete(id_)
