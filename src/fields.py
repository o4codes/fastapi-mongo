from bson import ObjectId


class PyObjectId(ObjectId):
    """Custom ObjectId type"""

    @classmethod
    def __get_validators__(cls):
        """Get validators for PyObjectId"""
        yield cls.validate

    @classmethod
    def validate(cls, v):
        """validate entered string is an ObjectId"""
        if not ObjectId.is_valid(v):
            raise ValueError("Invalid objectid")
        return ObjectId(v)

    @classmethod
    def __modify_schema__(cls, field_schema):
        field_schema.update(type="string")
