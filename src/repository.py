from typing import Any, Dict, List, Optional, Tuple, Type, Union

from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorCollection
from pymongo.collection import ReturnDocument

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

    async def delete(self, id_: ObjectId) -> bool:
        """Deletes Entity from database

        Args:
            id_[ObjectId]: primary key of entity to delete
        """
        result = await self.collection.delete_one({"_id": id_})
        return result.deleted_count > 0

    async def nested_list(
        self,
        main_doc_id: ObjectId,
        field: str,
        nested_model_class: Type[DbModel],
        sort: Optional[List[Tuple[str, int]]] = None,
        limit: Optional[int] = None,
    ) -> List[DbModel]:
        """
        Lists nested documents in MongoDB.

        Args:
            main_doc_id (ObjectId): The ID of the main document.
            field (str): The name of the field to list the nested documents from.
            nested_model_class (Optional[Type]): The class to use to serialize the nested documents.
                If None, returns the data as dictionaries.
            sort (Optional[List[Tuple[str, int]]]): A list of sort keys and sort orders.
                Example: [('name', pymongo.ASCENDING), ('age', pymongo.DESCENDING)]
            limit (Optional[int]): The maximum number of nested documents to return.

        Returns:
            A list of the nested documents.
        """
        query = {"_id": main_doc_id}
        pipeline = [{"$match": query}, {"$unwind": f"${field}"}]
        if sort:
            pipeline.append({"$sort": {k: v for k, v in sort}})
        if limit:
            pipeline.append({"$limit": limit})
        pipeline.append(
            {
                "$group": {
                    "_id": "$_id",
                    "count": {"$sum": 1},
                    field: {"$push": f"${field}"},
                }
            }
        )
        result = await self.collection.aggregate(pipeline).next()

        if result is None:
            return []

        nested_docs = result[field]
        nested_docs = [nested_model_class(**doc) for doc in nested_docs]
        return nested_docs

    async def nested_count(self, main_doc_id: ObjectId, field: str) -> int:
        """
        Returns the number of nested documents in MongoDB.

        Args:
            main_doc_id (ObjectId): The ID of the main document.
            field (str): The name of the field to count the nested documents from.

        Returns:
            The number of nested documents.
        """
        query = {"_id": main_doc_id}
        pipeline = [
            {"$match": query},
            {"$unwind": f"${field}"},
            {"$group": {"_id": "$_id", "count": {"$sum": 1}}},
        ]
        result = await self.collection.aggregate(pipeline).next()

        if result is None:
            return 0

        return result["count"]

    async def nested_create(
        self,
        main_doc_id: ObjectId,
        field: str,
        data: Union[Dict[str, Any], str, int],
        nested_model_class: Type[DbModel],
    ) -> Optional[DbModel]:
        """
        Creates a nested document in MongoDB.

        Args:
            main_doc_id (ObjectId): The ID of the main document.
            field (str): The name of the field in which to create the nested document.
            data (Union[Dict[str, Any], str, int]): The data for the nested document.
            nested_model_class (Optional[Type]): The class to use to serialize the nested document. If None,
                saves the data as is.

        Returns:
            The deserialized created nested document or None if creation failed.
        """
        if isinstance(data, dict):
            nested_doc_data = data
        elif isinstance(data, str) or isinstance(data, int):
            nested_doc_data = data
        else:
            raise ValueError("Data must be a dictionary, string, or integer")

        result = await self.collection.update_one(
            {"_id": main_doc_id}, {"$push": {field: nested_doc_data}}
        )

        if result.matched_count == 0:
            return None

        nested_doc = nested_model_class(**nested_doc_data)
        return nested_doc

    async def nested_get(
        self,
        main_doc_id: ObjectId,
        nested_doc_id: Optional[ObjectId],
        filter_: Optional[Dict[str, Any]],
        field: str,
        nested_model_class: Type[DbModel],
    ) -> Optional[DbModel]:
        """
        Retrieves a nested document from MongoDB.

        Args:
            main_doc_id (ObjectId): The ID of the main document.
            filter_ (Optional[Dict[str, Any]]): Optional filter to search for documents.
            field (str): The name of the field containing the nested document.
            nested_doc_id (Optional[ObjectId]): The ID of the nested document to retrieve. If None,
                retrieves all nested documents.
            nested_model_class (Optional[Type]): The class to use to deserialize the retrieved nested
                document(s). If None, returns the retrieved documents as dictionaries.

        Returns:
            The deserialized nested document or a list of deserialized nested documents if nested_doc_id is None,
            or None if the document is not found.
        """
        filter_ = filter_ or {}
        filter_["_id"] = main_doc_id
        filter_[f"{field}._id"] = nested_doc_id
        projection = {field: {"$elemMatch": {"_id": nested_doc_id}}}

        doc = await self.collection.find_one(filter_, projection=projection)
        if doc is None:
            return None

        nested_doc = doc[field]
        nested_doc = nested_doc[0]
        nested_doc = nested_model_class(**nested_doc)
        return nested_doc

    async def nested_update(
        self,
        main_doc_id: ObjectId,
        nested_doc_id: ObjectId,
        field: str,
        update: dict[str, Any],
        nested_model_class: Type,
        filter_: Optional[dict[str, Any]] = None,
        upsert: bool = False,
    ) -> Optional[Type]:
        """
        Updates a nested document in MongoDB

        Args:
            main_doc_id (ObjectId): The ID of the main document
            filter_ (Optional[Dict[str, Any]]): Optional filter to search for documents
            field (str): The name of the field containing the nested document
            nested_doc_id (ObjectId): The ID of the nested document to update
            update (Dict[str, Any]): The update to apply to the nested document
            nested_model_class (Type): The class to use to deserialize the updated nested document
            upsert (bool): Whether to create the nested document if it doesn't exist

        Returns:
            The deserialized updated nested document, or None if the document is not found
        """
        filter_ = filter_ or {}
        filter_ = {"_id": main_doc_id, **filter_, f"{field}._id": nested_doc_id}

        update_query = {"$set": {f"{field}.$.{k}": v for k, v in update.items()}}

        options = {
            "return_document": ReturnDocument.AFTER,
            "projection": {field: {"$elemMatch": {"_id": nested_doc_id}}},
        }

        updated_doc = await self.collection.find_one_and_update(
            filter_, update_query, **options, upsert=upsert
        )

        if updated_doc is None:
            return None

        nested_doc = next(
            (item for item in updated_doc[field] if item["_id"] == nested_doc_id), None
        )
        if nested_doc is None:
            return None

        return nested_model_class(**nested_doc)

    async def nested_remove(
        self,
        main_doc_id: ObjectId,
        nested_doc_id: ObjectId,
        filter_: Optional[Dict[str, Any]],
        field: str,
    ) -> Optional[Type]:
        """
        Removes a nested document from MongoDB

        Args:
            main_doc_id (ObjectId): The ID of the main document
            filter_ (Optional[Dict[str, Any]]): Optional filter to search for documents
            field (str): The name of the field containing the nested document
            nested_doc_id (ObjectId): The ID of the nested document to remove

        Returns:
            The deserialized removed nested document, or None if the document is not found
        """
        filter_ = filter_ or {}
        filter_ = {"_id": main_doc_id, **filter_, f"{field}._id": nested_doc_id}

        options = {
            "return_document": ReturnDocument.AFTER,
            "projection": {field: {"$elemMatch": {"_id": nested_doc_id}}},
        }

        removed_doc = await self.collection.find_one_and_update(
            filter_, {"$pull": {field: {"_id": nested_doc_id}}}, **options
        )

        return removed_doc is None
