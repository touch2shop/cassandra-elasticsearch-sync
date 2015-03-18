from app.core.exception.sync_exception import SyncException


class InvalidSchemaException(SyncException):

    def __init__(self, message, identifier=None):
        super(InvalidSchemaException, self).__init__(message, identifier)
