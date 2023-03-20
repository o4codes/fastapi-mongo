from typing import Optional, Type, Union

from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorCollection
from pymongo.results import UpdateResult

from .models import DbModel


class BaseRepository:
    model_klass: Type[DbModel]
    collection_name: str

    def __init__(self, database):
        self.collection: AsyncIOMotorCollection = database[self.collection_name]

    async def list(
        self, size: Optional[int], page: Optional[int], **filter_kwargs
    ) -> tuple[int, list[DbModel]]:
        """Produces a list of entities of model class

        Args:
            size [int]: optional size used to paginate results from database
            page [int]: optional page used to denote page number to limit results from database
            filter_kwargs [dict]: keyword values used to search for entities on database

        Returns:
            tuple[int, list[DbModel]: tuple of total results and paginated count
        """
        db_results = self.collection.find(filter_kwargs)
        total_count = await self.collection.count_documents(filter_kwargs)
        if size and page:
            if page == 1:
                db_results = await db_results.limit(size).to_list(length=size)
            else:
                db_results = (
                    await db_results.skip(size * page).limit(size).to_list(length=size)
                )
        objects = [self.model_klass(**result) for result in db_results]
        return total_count, objects

    async def get(self, id_: ObjectId) -> Optional[DbModel]:
        """Retrieves a single entity from database

        Args:
            id_ [ObjectId]: id of collection record to fetch

        Returns:
            entity[model_klass]: instance of model klass being queried
            None: When No record is found
        """
        db_result = await self.collection.find_one({"_id": id_})
        if db_result:
            return self.model_klass(**db_result)
        return None

    async def search(self, many: bool = False, **filter_kwargs):
        """Searches for item that match filtered property

        Args:
            many [bool]: boolean to indicate result for many or single
            filter_kwargs [dict]: keyword values of fieds for filtering
        """
        result_count = self.collection.count_documents(filter_kwargs)
        db_result: Union[dict, list] = (
            await self.collection.find_one(filter_kwargs)
            if not many
            else await self.collection.find(filter_kwargs).to_list(result_count)
        )
        if db_result:
            return (
                self.model_klass(**db_result)
                if not many
                else [self.model_klass(**result) for result in db_result]
            )
        return None

    async def count(self, **filter_kwargs) -> int:
        """Gets the count of queried entities

        Args:
            filter_kwargs (dict): values to be used for filtering

        Returns:
            int: count of entities found
        """
        return await self.collection.count_documents(filter_kwargs)

    async def create(self, model_instance: DbModel):
        """
        Adds entity to collection records

        Args:
            model_instance[model_klass]: Instance of model_klass to add

        Returns:
            model_instance[model_klass]: Model Instance being added
        """
        db_insert = await self.collection.insert_one(
            model_instance.dict(exclude={"id"})
        )
        db_result = await self.get(db_insert.inserted_id)
        return db_result

    async def nested_create(
        self,
        id_: ObjectId,
        filter_: Optional[dict],
        field: str,
        data: Union[str, int, dict, list],
    ):
        """
        Creates Nested data into a mongodb collection

        Args:
            id_ [ObjectId]: primary key of main document
            filter_ [dict]: conditions to search for before creation
            field [str]: field to inseert the nested data into
            data [Union[str, int, dict, list]]: nested data to be inserted
        """
        if filter_ is None:
            filter_ = {}

        db_insert: UpdateResult = await self.collection.update_one(
            {"_id": id_, **filter_}, {"$push": {field: data}}
        )
        if db_insert.modified_count == 0:
            return None
        return await self.get(id_)

    async def update(self, id_: ObjectId, model_instance: DbModel):
        """
        Updates a model instance in the colelction

        Args:
            id_ (str): id database model
            model_instance (DbModel): update data

        Returns:
            model_instance[model_klass]: Update Model Instance
        """
        await self.collection.update_one(
            {"_id": id_}, {"$set": model_instance.dict(exclude={"id"})}
        )
        db_result = await self.get(id_)
        return db_result

    async def nested_update(
        self,
        id_: ObjectId,
        filter_: Optional[dict],
        field: str,
        data: Union[str, int, dict, list],
    ):
        """
        Creates Nested data into a mongodb collection

        Args:
            id_ [ObjectId]: primary key of main document
            filter_ [dict]: conditions to search for before creation
            field [str]: field to inseert the nested data into
            data [Union[str, int, dict, list]]: nested data to be inserted
        """
        if filter_:
            filter_ = {}
        db_result: UpdateResult = await self.collection.update_one(
            {"_id": id_, **filter_}, {"$set": {field: data}}
        )
        if db_result.matched_count == 0:
            return None
        return await self.get(id_)

    async def delete(self, id_: ObjectId) -> bool:
        """Deletes Entity from database

        Args:
            id_[ObjectId]: primary key of entity to delete
        """
        result = await self.collection.delete_one({"_id": id_})
        return result.deleted_count > 0

    async def nested_delete(
        self,
        id_: ObjectId,
        filter_: Optional[dict],
        field: str,
        data: Union[str, int, dict],
    ) -> bool:
        """
        Creates Nested data into a mongodb collection

        Args:
        id_ [ObjectId]: primary key of main document
        filter_ [dict]: conditions to search for before creation
        field [str]: field to inseert the nested data into
        data [Union[str, int, dict, list]]: nested data to be inserted
        """
        if filter_ is None:
            filter_ = {}

        db_insert: UpdateResult = await self.collection.update_one(
            {"_id": id_, **filter_}, {"$pull": {field: data}}
        )
        return db_insert.modified_count > 0
