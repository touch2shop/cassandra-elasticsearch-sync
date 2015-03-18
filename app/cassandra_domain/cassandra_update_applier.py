from app.cassandra_domain.invalid_cassandra_schema_exception import InvalidCassandraSchemaException
from app.cassandra_domain.store.cassandra_client import CassandraClient
from app.cassandra_domain.store.cassandra_document_store import CassandraDocumentStore
from app.core.abstract_update_applier import AbstractUpdateApplier


class CassandraUpdateApplier(AbstractUpdateApplier):

    def __init__(self, cassandra_cluster, settings):
        document_store = CassandraDocumentStore(cassandra_cluster,
                settings.cassandra_id_column_name, settings.cassandra_timestamp_column_name)
        super(CassandraUpdateApplier, self).__init__(document_store)
        self._cassandra_client = CassandraClient(cassandra_cluster)

    def _check_namespace_and_table_exists(self, identifier):
        if not self._cassandra_client.keyspace_exists(identifier.namespace):
            raise InvalidCassandraSchemaException(identifier=identifier,
                message="Keyspace does not exist on Cassandra.")
        if not self._cassandra_client.table_exists(identifier.namespace, identifier.table):
            raise InvalidCassandraSchemaException(identifier=identifier,
                message="Table does not exist on Cassandra.")
