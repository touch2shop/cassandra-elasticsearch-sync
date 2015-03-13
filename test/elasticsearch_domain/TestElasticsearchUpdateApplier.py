from uuid import uuid4
import pytest
from time import time
from app.core.Identifier import Identifier
from app.core.Update import Update
from app.core.UpdateEvent import UpdateEvent
from app.core.exception.InvalidElasticsearchSchemaException import InvalidElasticsearchSchemaException
from app.elasticsearch_domain.ElastisearchUpdateApplier import ElasticsearchUpdateApplier


@pytest.fixture(scope="module")
def update_applier(elasticsearch_client):
    return ElasticsearchUpdateApplier(elasticsearch_client)


# noinspection PyClassHasNoInit,PyShadowingNames,PyMethodMayBeStatic
class TestElasticsearchUpdateApplier:

    def test_fail_if_index_does_not_exist(self, update_applier):
        bogus_update = self.build_update(namespace="invalid", table="product", key=str(uuid4()),
                                         timestamp=time())

        with pytest.raises(InvalidElasticsearchSchemaException) as e:
            update_applier.apply_updates([bogus_update])
        assert e.value.identifier == bogus_update.identifier

    def test_fail_if_type_does_not_exist(self, update_applier, elasticsearch_fixture_index):
        bogus_update = self.build_update(namespace=elasticsearch_fixture_index, table="invalid", key=str(uuid4()),
                                         timestamp=time())

        with pytest.raises(InvalidElasticsearchSchemaException) as e:
            update_applier.apply_updates([bogus_update])
        assert e.value.identifier == bogus_update.identifier

    def build_update(self, namespace, table, key, timestamp):
        identifier = Identifier(namespace, table, key)
        event = UpdateEvent(identifier=identifier, timestamp=timestamp)
        update = Update(event)
        return update

    def test_fail_if_mapping_does_not_store_timestamp_for_document(self, update_applier,
                                                                   elasticsearch_client, elasticsearch_fixture_index):
        _index = elasticsearch_fixture_index
        _type = "type_without_timestamp"
        _id = uuid4()

        bogus_mapping = {"_timestamp": {"enabled": True, "store": False}}
        elasticsearch_client.indices.put_mapping(index=_index, doc_type=_type, body=bogus_mapping)
        elasticsearch_client.index(index=_index, doc_type=_type, id=_id, body={"foo": "bar"})

        update = self.build_update(namespace=_index, table=_type, key=str(_id), timestamp=time())

        with pytest.raises(InvalidElasticsearchSchemaException) as e:
            update_applier.apply_updates([update])
        assert e.value.identifier == update.identifier
        assert "Could not retrieve timestamp for Elasticsearch document" in e.value.message
