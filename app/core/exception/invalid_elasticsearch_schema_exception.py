from app.core.exception.sync_exception import SyncException


class InvalidElasticsearchSchemaException(SyncException):

    def __init__(self, message, identifier=None):
        super(InvalidElasticsearchSchemaException, self).__init__(message, identifier)
