from datetime import datetime
from enum import Enum
from typing import Optional, Type, Union

from bson import ObjectId
from pydantic import BaseModel, Field, root_validator

from .fields import PyObjectId


class DbModel(BaseModel):
    id: PyObjectId = Field(default_factory=PyObjectId, alias="_id")
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: Optional[datetime]

    class Config:
        allow_population_by_field_name = True
        arbitrary_types_allowed = True
        json_encoders = {ObjectId: str, datetime: lambda v: v.isoformat()}


class PaginationModel(BaseModel):
    total_count: int
    total_pages: Optional[int]
    page: int
    size: int
    data: list[Type[BaseModel]]

    @root_validator
    def paginate(cls, values: dict):
        total_count = values["total_count"]
        size = values["size"]
        values["total_pages"] = (
            0
            if total_count == 0
            else 1
            if total_count <= size
            else total_count // size
            if (total_count % size) == 0
            else (total_count // size) + 1
        )
        return values

    class Config:
        allow_population_by_field_name = True
        arbitrary_types_allowed = True
        json_encoders = {ObjectId: str, datetime: lambda v: v.isoformat()}


class ErrorModelStatus(str, Enum):
    SUCCESS = "success"
    FAILED = "failed"


class ErrorModel(BaseModel):
    status: ErrorModelStatus
    message: str
    details: Union[str, list, dict]
    timestamp: float = datetime.now().timestamp()
