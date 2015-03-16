from decimal import Decimal
from time import time
from uuid import uuid4
from test.fixtures.product import ProductFixture


# noinspection PyMethodMayBeStatic, PyClassHasNoInit
class TestAbstractElasticsearchStore:

    def test_create_and_read(self, product_fixture_elasticsearch_store):
        _id = uuid4()
        created_product = ProductFixture(_id=_id, name="jeans", description="cool jeans", quantity=5,
                                         price=Decimal("99.99"), enabled=True, timestamp=time())
        product_fixture_elasticsearch_store.create(created_product)

        read_product = product_fixture_elasticsearch_store.read(_id)
        assert created_product.name == read_product.name
        assert created_product.description == read_product.description
        assert created_product.quantity == read_product.quantity
        assert created_product.id == read_product.id
        assert created_product.price == read_product.price
        assert abs(created_product.timestamp - read_product.timestamp) < 0.001

    def test_read_not_found(self, product_fixture_elasticsearch_store):
        assert product_fixture_elasticsearch_store.read(uuid4()) is None

    def test_full_update(self, product_fixture_elasticsearch_store):
        _id = uuid4()

        product = ProductFixture(_id=_id, timestamp=time(), name="jeans", description="cool jeans", quantity=5,
                                 price=Decimal("49.99"), enabled=False)
        product_fixture_elasticsearch_store.create(product)

        updated_name = "shirt"
        updated_description = "red t-shirt"
        updated_quantity = 10
        updated_price = Decimal("99.99")
        updated_enabled = True

        product.name = updated_name
        product.description = updated_description
        product.quantity = updated_quantity
        product.price = updated_price
        product.enabled = updated_enabled
        product.timestamp = time()
        product_fixture_elasticsearch_store.update(product)

        read_product = product_fixture_elasticsearch_store.read(_id)
        assert read_product.name == updated_name
        assert read_product.description == updated_description
        assert read_product.quantity == updated_quantity
        assert read_product.price == updated_price
        assert read_product.enabled == updated_enabled

    def test_partial_update(self, product_fixture_elasticsearch_store):
        _id = uuid4()

        original_name = "jeans"
        original_description = "cool jeans"
        original_quantity = 5

        product = ProductFixture(_id=_id, timestamp=time(), name=original_name,
                                 description=original_description, quantity=original_quantity)
        product_fixture_elasticsearch_store.create(product)

        updated_description = "red t-shirt"
        updated_quantity = 10

        product.description = updated_description
        product.quantity = updated_quantity
        product.timestamp = time()
        product_fixture_elasticsearch_store.update(product)

        read_product = product_fixture_elasticsearch_store.read(_id)
        assert read_product.name == original_name
        assert read_product.description == updated_description
        assert read_product.quantity == updated_quantity

    def test_delete(self, product_fixture_elasticsearch_store):
        _id = uuid4()

        product = ProductFixture(_id=_id, timestamp=time(), name="jeans", description="cool jeans", quantity=5)
        product_fixture_elasticsearch_store.create(product)

        assert product_fixture_elasticsearch_store.read(_id)

        product_fixture_elasticsearch_store.delete(_id)

        assert not product_fixture_elasticsearch_store.read(_id)
