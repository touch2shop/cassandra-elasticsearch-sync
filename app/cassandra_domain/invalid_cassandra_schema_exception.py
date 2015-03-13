from app.core.exception.sync_exception import SyncException


class InvalidCassandraSchemaException(SyncException):

    def __init__(self, message, identifier=None):
        super(InvalidCassandraSchemaException, self).__init__(message, identifier)
