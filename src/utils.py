import secrets
from datetime import datetime, timedelta

from jose import JWTError, jwt
from passlib.context import CryptContext

from src.config import settings
from src.libs import exceptions

settings_lib = settings.Settings()
pwd_context = CryptContext(schemes=[settings_lib.hash_scheme], deprecated="auto")


def get_random_string() -> str:
    return str(secrets.token_hex(16))


def get_string_hash(value: str):
    return pwd_context.hash(value)


def verify_hash(hash_value: str, plain_value: str):
    return pwd_context.verify(plain_value, hash_value)


def create_access_token(data: dict) -> str:
    """Generates JWT

    Args:
        data (dict): data to be encoded

    Returns:
        str: encoded token
    """
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(
        minutes=settings_lib.access_token_expire_minutes
    )
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(
        to_encode, settings_lib.secret_key, algorithm=settings_lib.token_algorithm
    )
    return encoded_jwt


def decode_access_token(token: str) -> dict:
    """decodes jwt access token

    Args:
        token (str): Token

    Returns:
        dict: data decoded from token

    Raises:
        ForbiddenException: when jwt cannot be decoded
    """
    try:
        payload = jwt.decode(
            token, settings_lib.secret_key, algorithms=[settings_lib.token_algorithm]
        )
        return payload
    except JWTError as e:
        raise exceptions.ForbiddenException(str(e))
