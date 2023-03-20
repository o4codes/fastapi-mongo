import io
from typing import NoReturn, Optional, Type, Union

from bson import ObjectId
from gridfs import errors as gridfs_errors
from motor.motor_asyncio import AsyncIOMotorDatabase, AsyncIOMotorGridFSBucket

from .exceptions import NotFoundException


class FileSystem:
    """Serves as a file system handler for storing files.
    Implementation uses gridfs for storing files directly on mongodb
    """

    def __init__(self, database: AsyncIOMotorDatabase):
        self.fs = AsyncIOMotorGridFSBucket(database)

    async def upload(self, file_name: str, file_bytes: bytes) -> ObjectId:
        """Uploads file bytes to file system

        Args:
            file_name (str): title/name of the file be uploaded
            file_bytes (bytes): byte content of file to be uploaded

        Returns:
            ObjectId: id of the stored file
        """
        file_id = await self.fs.upload_from_stream(file_name, file_bytes)
        return file_id

    async def download(self, file_id: ObjectId) -> io.BytesIO:
        """Retrieves content of a file and saves in io buffer

        Args:
            file_id (ObjectId): Id of the file

        Raises:
            NotFoundException: When no file is fouund

        Returns:
            io.BytesIO: retrieved file buffer of stored file
        """
        try:
            file_stream = io.BytesIO()
            await self.fs.download_to_stream(file_id, file_stream)
            file_stream.seek(0)
            return file_stream
        except gridfs_errors.NoFile:
            raise NotFoundException("File not found")

    async def delete(self, file_id: ObjectId) -> None:
        """Deletes a file from the file system

        Args:
            file_id (ObjectId): Id of the file

        Raises:
            NotFoundException: When no such file with id is found

        Returns:
            None: On succesful delete
        """
        try:
            await self.fs.delete(file_id)
        except gridfs_errors.NoFile:
            raise NotFoundException("File not found")
