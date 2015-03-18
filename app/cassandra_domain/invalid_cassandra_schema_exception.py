from app.core.exception.invalid_schema_exception import InvalidSchemaException


class InvalidCassandraSchemaException(InvalidSchemaException):

    def __init__(self, message, identifier=None):
        super(InvalidCassandraSchemaException, self).__init__(message, identifier)
