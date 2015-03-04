import uuid
import pytest
from test.cassandra.fixtures import ProductFixture
from app.cassandra.CassandraUpdateFetcher import CassandraUpdateFetcher


@pytest.fixture(scope="function")
def setup(cassandra_log_entry_store, product_fixture_store):
    product_fixture_store.delete_all()
    cassandra_log_entry_store.delete_all()


@pytest.fixture(scope="function")
def product_fixtures(product_fixture_store):

    products = list()
    products.append(ProductFixture(uuid.uuid4(), "navy polo shirt", 5, "great shirt, great price!"))
    products.append(ProductFixture(uuid.uuid4(), "cool red shorts", 7, "perfect to go to the beach"))
    products.append(ProductFixture(uuid.uuid4(), "black DC skater shoes", 10, "yo!"))

    for product in products:
        product_fixture_store.create(product)

    return products


@pytest.fixture("module")
def cassandra_update_fetcher(cassandra_log_entry_store):
    return CassandraUpdateFetcher(cassandra_log_entry_store)


# noinspection PyShadowingNames,PyMethodMayBeStatic,PyClassHasNoInit
@pytest.mark.slow
@pytest.mark.usefixtures("create_product_fixture_schema", "setup")
class TestCassandraUpdateFetcher:

    def test_fetch_updates_for_initial_product_fixtures(self, cassandra_update_fetcher, product_fixtures,
                                                        cassandra_log_entry_store):

        updates = cassandra_update_fetcher.fetch_updates()

        updates_by_product_id = {}
        for update in updates:
            product_id = update.identifer.key
            if product_id not in updates_by_product_id:
                updates_by_product_id[product_id] = []
            updates_by_product_id[product_id].append(update)

        for product_id in updates_by_product_id.keys():
            product_updates = updates_by_product_id[product_id]
            assert len(product_updates) is 1
            product_update = product_updates[0]
            assert len(product_update.field_names) is 3
            assert product_update.field_names is 3