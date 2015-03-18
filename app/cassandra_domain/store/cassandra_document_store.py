from decimal import Decimal
from operator import attrgetter
from uuid import UUID
import arrow
import cassandra.cqltypes

from app.cassandra_domain.invalid_cassandra_schema_exception import InvalidCassandraSchemaException
from app.cassandra_domain.store.cassandra_client import CassandraClient
from app.core.abstract_document_store import AbstractDocumentStore
from app.core.model.document import Document
from app.core.util.timestamp_util import TimestampUtil


_DEFAULT_ID_COLUMN_NAME = "id"
_DEFAULT_TIMESTAMP_COLUMN_NAME = "timestamp"


class CassandraDocumentStore(AbstractDocumentStore):

    def __init__(self, cluster, id_column_name=_DEFAULT_ID_COLUMN_NAME,
                 timestamp_column_name=_DEFAULT_TIMESTAMP_COLUMN_NAME):
        self._client = CassandraClient(cluster)
        self._id_column_name = id_column_name
        self._timestamp_column_name = timestamp_column_name

    def read(self, identifier):
        statement = self._client.prepare_statement(
            """
            SELECT * FROM %s.%s WHERE %s=%s
            """
            % (identifier.namespace, identifier.table, self._id_column_name, identifier.key)
        )
        rows = self._client.execute(statement)
        return self._to_document(identifier, rows)

    def create(self, document):
        identifier = document.identifier
        statement = self._client.prepare_statement(
            """
            INSERT INTO %s.%s
                   (%s,%s,%s)
            VALUES (%s,?,%s)
            """ % (identifier.namespace, identifier.table,
                   self._id_column_name, self._timestamp_column_name,
                   self.__get_field_names_for_insert_cql_string(document),
                   identifier.key, self.__get_question_marks_string(document))
        )

        arguments = [self.__get_timestamp(document)]
        arguments.extend(self.__get_field_values(document, statement))

        self._client.execute(statement, arguments)

    def update(self, document):
        identifier = document.identifier
        statement = self._client.prepare_statement(
            """
            UPDATE %s.%s
            SET %s=?,%s
            WHERE %s=%s
            """ % (identifier.namespace, identifier.table,
                   self._timestamp_column_name, self.__get_field_names_for_update_cql_string(document),
                   self._id_column_name, identifier.key)
        )
        arguments = [self.__get_timestamp(document)]
        arguments.extend(self.__get_field_values(document, statement))

        self._client.execute(statement, arguments)

    def delete(self, identifier):
        statement = self._client.prepare_statement(
            """
            DELETE FROM %s.%s
            WHERE %s=%s
            """
            % (identifier.namespace, identifier.table, self._id_column_name, identifier.key)
        )
        self._client.execute(statement)

    @classmethod
    def __get_timestamp(cls, document):
        return TimestampUtil.seconds_to_milliseconds(document.timestamp)

    @classmethod
    def __get_field_names_for_insert_cql_string(cls, document):
        names = []
        for field in document.fields:
            names.append(field.name)
        return ",".join(names)

    @classmethod
    def __get_field_names_for_update_cql_string(cls, document):
        names = []
        for field in document.fields:
            names.append("%s=?" % field.name)
        return ",".join(names)

    @classmethod
    def __get_question_marks_string(cls, document):
        question_marks = []
        for _ in document.fields:
            question_marks.append("?")
        return ",".join(question_marks)

    @classmethod
    def __get_column_types(cls, statement):
        types = dict()
        for metadata in statement.column_metadata:
            types[metadata[2].lower()] = metadata[3]
        return types

    @classmethod
    def __get_field_values(cls, document, statement):
        column_types = cls.__get_column_types(statement)
        values = []
        for field in document.fields:
            value = cls.__serialize_field_value(field.value, column_types[field.name.lower()])
            values.append(value)
        return values

    @classmethod
    def __serialize_field_value(cls, value, column_type):
        if isinstance(value, str):
            if column_type == cassandra.cqltypes.DecimalType:
                return Decimal(value)
            if column_type == cassandra.cqltypes.UUIDType:
                return UUID(value)
            if column_type == cassandra.cqltypes.TimeUUIDType:
                return UUID(value)
        return value

    def _to_document(self, identifier, rows):
        if len(rows) == 0:
            return None
        elif len(rows) > 1:
            raise InvalidCassandraSchemaException(identifier=identifier,
                    message=("More than one row found for entity on Cassandra. " +
                             "Please make sure the schema has a single primary key column with name '%s'. ")
                                % self._id_column_name)

        row = rows[0]

        if not hasattr(row, self._timestamp_column_name):
                raise InvalidCassandraSchemaException(identifier=identifier,
                        message=("No timestamp column found for entity on Cassandra. " +
                                 "Please make sure the schema has a timestamp column with name '%s'. ")
                                 % self._timestamp_column_name)

        document = Document()
        document.identifier = identifier

        timestamp = attrgetter(self._timestamp_column_name)(row)
        if timestamp:
            document.timestamp = arrow.get(timestamp).float_timestamp

        for field_name in row.__dict__:
            if field_name != self._timestamp_column_name and field_name != self._id_column_name:
                field_value = attrgetter(field_name)(row)
                document.add_field(field_name, field_value)

        return document
