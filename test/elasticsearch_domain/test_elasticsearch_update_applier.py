from decimal import Decimal
from uuid import uuid4
from time import time, sleep

import pytest

from app.core.identifier import Identifier
from app.core.update import Update
from app.core.field import Field
from app.elasticsearch_domain.invalid_elasticsearch_schema_exception import InvalidElasticsearchSchemaException
from app.elasticsearch_domain.elasticsearch_update_applier import ElasticsearchUpdateApplier
from test.fixtures.product import ProductFixture


@pytest.fixture(scope="module")
def update_applier(elasticsearch_client):
    return ElasticsearchUpdateApplier(elasticsearch_client)


def build_update(namespace, table, key, timestamp, fields=None, is_delete=False):
    identifier = Identifier(namespace, table, key)
    return Update(identifier=identifier, timestamp=timestamp, fields=fields, is_delete=is_delete)


def build_fields(**kwargs):
    fields = []
    for (name, value) in kwargs.items():
        fields.append(Field(name, value))
    return fields


# noinspection PyClassHasNoInit,PyShadowingNames,PyMethodMayBeStatic
class TestElasticsearchUpdateApplier:

    def test_fail_if_index_does_not_exist(self, update_applier):
        bogus_update = build_update(namespace="invalid", table="product", key=str(uuid4()), timestamp=time())

        with pytest.raises(InvalidElasticsearchSchemaException) as e:
            update_applier.apply_update(bogus_update)
        assert e.value.identifier == bogus_update.identifier

    def test_fail_if_type_does_not_exist(self, update_applier, elasticsearch_fixture_index):
        bogus_update = build_update(namespace=elasticsearch_fixture_index, table="invalid", key=str(uuid4()),
                                    timestamp=time())

        with pytest.raises(InvalidElasticsearchSchemaException) as e:
            update_applier.apply_update(bogus_update)
        assert e.value.identifier == bogus_update.identifier

    def test_fail_if_mapping_does_not_have_timestamp_enabled(self, update_applier,
                                                             elasticsearch_client, elasticsearch_fixture_index):
        _index = elasticsearch_fixture_index
        _type = "type_without_timestamp"
        _id = uuid4()

        bogus_mapping = {"_timestamp": {"enabled": False}}
        elasticsearch_client.indices.put_mapping(index=_index, doc_type=_type, body=bogus_mapping)
        elasticsearch_client.index(index=_index, doc_type=_type, id=_id, body={"foo": "bar"})

        update = build_update(namespace=_index, table=_type, key=str(_id), timestamp=time())

        with pytest.raises(InvalidElasticsearchSchemaException) as e:
            update_applier.apply_update(update)
        assert e.value.identifier == update.identifier
        assert "Could not retrieve timestamp for Elasticsearch document" in e.value.message

    def test_apply_save_update_to_nonexistent_document(self, update_applier, elasticsearch_fixture_index,
                                                       product_fixture_table, product_fixture_elasticsearch_store):

        _index = elasticsearch_fixture_index
        _type = product_fixture_table
        _id = uuid4()

        name = "t-shirt"
        description = "cool red t-shirt"
        quantity = 5
        price = Decimal("99.99")
        enabled = True

        update = build_update(namespace=_index, table=_type, key=str(_id), timestamp=time(),
                              fields=build_fields(name=name, enabled=enabled, description=description,
                                                  quantity=quantity, price=price))
        update_applier.apply_update(update)

        created = product_fixture_elasticsearch_store.read(_id)
        assert created
        assert created.name == name
        assert created.description == description
        assert created.quantity == quantity
        assert created.enabled == enabled
        assert created.price == price

    def test_apply_delete_update_to_nonexistent_document(self, update_applier, elasticsearch_fixture_index,
                                                         product_fixture_table, product_fixture_elasticsearch_store):

        _index = elasticsearch_fixture_index
        _type = product_fixture_table
        _id = uuid4()

        delete_update = build_update(namespace=_index, table=_type, key=str(_id), timestamp=time(), is_delete=True,
                                     fields=build_fields(name="t-shirt", description="cool red t-shirt", quantity=5))
        update_applier.apply_update(delete_update)

        assert not product_fixture_elasticsearch_store.read(_id)

    def test_apply_full_save_update_to_existing_document(self, update_applier, elasticsearch_fixture_index,
                                                         product_fixture_table, product_fixture_elasticsearch_store):

        _index = elasticsearch_fixture_index
        _type = product_fixture_table
        _id = uuid4()

        product = ProductFixture(_id=_id, name="jeans", description="cool jeans", quantity=10,
                                 price=Decimal("49.99"), enabled=True)
        product_fixture_elasticsearch_store.create(product)

        updated_name = "t-shirt"
        updated_description = "cool red t-shirt"
        updated_quantity = 5
        updated_price = Decimal("69.99")
        updated_enabled = False

        update = build_update(namespace=_index, table=_type, key=str(_id), timestamp=time(),
                              fields=build_fields(name=updated_name,
                                                  description=updated_description,
                                                  quantity=updated_quantity,
                                                  price=updated_price,
                                                  enabled=updated_enabled))
        update_applier.apply_update(update)

        product = product_fixture_elasticsearch_store.read(_id)
        assert product.name == updated_name
        assert product.description == updated_description
        assert product.quantity == updated_quantity

    def test_apply_partial_save_update_to_existing_document(self, update_applier, elasticsearch_fixture_index,
                                                            product_fixture_table, product_fixture_elasticsearch_store):

        _index = elasticsearch_fixture_index
        _type = product_fixture_table
        _id = uuid4()

        original_name = "jeans"
        original_description = "cool jeans"
        original_quantity = 5
        original_price = Decimal("149.99")
        original_enabled = True

        product = ProductFixture(_id=_id, name=original_name,
                                 description=original_description, quantity=original_quantity,
                                 price=original_price, enabled=original_enabled)
        product_fixture_elasticsearch_store.create(product)

        updated_description = "cool red t-shirt"
        updated_quantity = 10

        update = build_update(namespace=_index, table=_type, key=str(_id), timestamp=time(),
                              fields=build_fields(description=updated_description,
                                                  quantity=updated_quantity))
        update_applier.apply_update(update)

        product = product_fixture_elasticsearch_store.read(_id)
        assert product.name == original_name
        assert product.price == original_price
        assert product.enabled == original_enabled
        assert product.description == updated_description
        assert product.quantity == updated_quantity

    def test_apply_delete_update_to_existing_document(self, update_applier, elasticsearch_fixture_index,
                                                      product_fixture_table, product_fixture_elasticsearch_store):
        _index = elasticsearch_fixture_index
        _type = product_fixture_table
        _id = uuid4()

        product = ProductFixture(_id=_id, name="jeans", description="cool jeans", quantity=5)
        product_fixture_elasticsearch_store.create(product)

        delete_update = build_update(namespace=_index, table=_type, key=str(_id), timestamp=time(), is_delete=True)
        update_applier.apply_update(delete_update)

        assert not product_fixture_elasticsearch_store.read(_id)

    def test_do_not_apply_update_to_existing_document_if_it_was_already_updated(self, update_applier,
            elasticsearch_fixture_index, product_fixture_table, product_fixture_elasticsearch_store):

        _index = elasticsearch_fixture_index
        _type = product_fixture_table
        _id = uuid4()

        original_name = "jeans"
        original_description = "cool jeans"
        original_quantity = 5

        product = ProductFixture(_id=_id, timestamp=time(),
                                 name=original_name, description=original_description, quantity=original_quantity)
        product_fixture_elasticsearch_store.create(product)

        updated_name = "t-shirt"
        updated_description = "cool red t-shirt"
        updated_quantity = 10

        obsolete_update = build_update(namespace=_index, table=_type, key=str(_id), timestamp=time(),
                                       fields=build_fields(name=updated_name,
                                                           description=updated_description,
                                                           quantity=updated_quantity))

        sleep(0.001)

        more_recently_updated_name = "shoes"
        more_recently_updated_description = "skater shoes"

        product.name = more_recently_updated_name
        product.description = more_recently_updated_description
        product.timestamp = time()
        product_fixture_elasticsearch_store.update(product)

        update_applier.apply_update(obsolete_update)

        product = product_fixture_elasticsearch_store.read(_id)
        assert product.name == more_recently_updated_name
        assert product.description == more_recently_updated_description
        assert product.quantity == original_quantity

    def only_update_if_fields_are_different(self, update_applier, elasticsearch_fixture_index,
                                            product_fixture_table, product_fixture_elasticsearch_store):

        _index = elasticsearch_fixture_index
        _type = product_fixture_table
        _id = uuid4()

        original_name = "jeans"
        original_description = "cool jeans"
        original_quantity = 5
        original_price = Decimal("149.99")
        original_enabled = True

        original_timestamp = time()

        product = ProductFixture(_id=_id, timestamp=original_timestamp, name=original_name,
                                 description=original_description, quantity=original_quantity,
                                 price=original_price, enabled=original_enabled)
        product_fixture_elasticsearch_store.create(product)

        sleep(0.001)

        update_timestamp = time()
        useless_update = build_update(namespace=_index, table=_type, key=str(_id), timestamp=update_timestamp,
                                      fields=build_fields(name=original_name,
                                                          description=original_description,
                                                          quantity=original_quantity,
                                                          enabled=original_enabled,
                                                          price=original_price))

        update_applier.apply_update(useless_update)

        read_product = product_fixture_elasticsearch_store.read(product)
        assert read_product.name == original_name
        assert read_product.description == original_description
        assert read_product.quantity == original_quantity
        assert read_product.timestamp == original_timestamp
