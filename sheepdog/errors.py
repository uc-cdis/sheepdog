"""
TODO
"""

import cdispyutils

from sheepdog.globals import (
    SUPPORTED_FORMATS,
)


class APIError(Exception):

    def __init__(self, message, code, json):
        super(APIError, self).__init__()
        self.message = message
        self.code = code
        self.json = json


class APINotImplemented(APIError):

    def __init__(self, message, code=501, json=None):
        super(APINotImplemented, self).__init__(message, code, json)


class AuthError(APIError):

    def __init__(self, message=None, code=403, json=None):
        if json is None:
            json = {}
        auth_message = "You don't have access to this data"
        if message is not None:
            auth_message += ': {}'.format(message)
        super(AuthError, self).__init__(auth_message, code, json)


class InternalError(APIError):

    def __init__(self, message=None, code=500):
        self.message = "Internal server error"
        if message:
            self.message += ': {}'.format(message)
        self.code = code


class InvalidTokenError(AuthError):

    def __init__(self):
        self.message = (
            "Your token is invalid or expired. Please get a new token from GDC"
            " Data Portal."
        )
        self.code = 403


class NotFoundError(APIError):

    def __init__(self, message):
        super(NotFoundError, self).__init__(message, 404, None)


class UserError(APIError):

    def __init__(self, message, code=400, json=None):
        if json is None:
            json = {}
        super(UserError, self).__init__(message, code, json)


class UnsupportedError(UserError):

    def __init__(self, file_format, code=400, json=None):
        if json is None:
            json = {}
        message = (
            "Format {} is not supported; supported formats are: {}."
            .format(file_format, ",".join(SUPPORTED_FORMATS))
        )
        super(UnsupportedError, self).__init__(message, code, json)


class ParsingError(Exception):
    pass


class SchemaError(Exception):

    def __init__(self, message, e=None):
        if e:
            log = cdispyutils.log.get_logger(__name__)
            log.exception(e)
        message = "{}: {}".format(message, e) if e else message
        super(SchemaError, self).__init__(message)
