import uuid
import pytest
import time
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
        bogus_identifier = Identifier(namespace="invalid", table="product", key=str(uuid.uuid4()))
        bogus_event = UpdateEvent(identifier=bogus_identifier, timestamp=time.time())
        bogus_update = Update(bogus_event)

        with pytest.raises(InvalidElasticsearchSchemaException) as e:
            update_applier.apply_updates([bogus_update])
        assert e.value.identifier == bogus_event.identifier

    def test_fail_if_type_does_not_exist(self, update_applier, elasticsearch_fixture_index):
        bogus_identifier = Identifier(namespace=elasticsearch_fixture_index, table="invalid", key=str(uuid.uuid4()))
        bogus_event = UpdateEvent(identifier=bogus_identifier, timestamp=time.time())
        bogus_update = Update(bogus_event)

        with pytest.raises(InvalidElasticsearchSchemaException) as e:
            update_applier.apply_updates([bogus_update])
        assert e.value.identifier == bogus_event.identifier
