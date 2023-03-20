from typing import Literal

from fastapi import status
from fastapi.exceptions import HTTPException


class BaseHTTPException(Exception):
    """Base Exception for all errors"""

    status_code: Literal

    def __init__(self, message):
        super().__init__(message)
        self.message = message


class NotFoundException(BaseHTTPException):
    """
    Exception for when a resource is not found
    """

    status_code = status.HTTP_404_NOT_FOUND

    def __init__(self, message):
        super().__init__(message)
        self.message = message

    def __str__(self):
        return self.message


class InternalServerException(BaseHTTPException):
    """
    Exception for when a database
    """

    status_code = status.HTTP_500_INTERNAL_SERVER_ERROR

    def __init__(self, message):
        super().__init__(message)
        self.message = message

    def __str__(self):
        return self.message


class BadRequest(BaseHTTPException):
    """
    Exception for when a request is bad
    """

    status_code = status.HTTP_400_BAD_REQUEST

    def __init__(self, message):
        super().__init__(message)
        self.message = message

    def __str__(self):
        return self.message


class ForbiddenException(BaseHTTPException):
    """
    Exception for when a user is forbidden from an action
    """

    status_code = status.HTTP_403_FORBIDDEN

    def __init__(self, message):
        super().__init__(message)
        self.message = message

    def __str__(self):
        return self.message


class UnauthorizedException(Exception):
    """
    Exception for when a user is not authorized
    """

    status_code = status.HTTP_401_UNAUTHORIZED

    def __init__(self, message):
        super().__init__(message)
        self.message = message

    def __str__(self):
        return self.message
