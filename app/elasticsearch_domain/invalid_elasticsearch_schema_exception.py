from app.core.exception.invalid_schema_exception import InvalidSchemaException


class InvalidElasticsearchSchemaException(InvalidSchemaException):

    def __init__(self, message, identifier=None):
        super(InvalidElasticsearchSchemaException, self).__init__(message, identifier)
