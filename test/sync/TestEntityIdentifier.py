import pytest

from app.sync.EntityIdentifier import EntityIdentifier


@pytest.fixture
def entity_identifier():
    return EntityIdentifier(namespace="namespace", table="table", key="key")


@pytest.fixture
def identical_entity_identifier():
    return EntityIdentifier(namespace="namespace", table="table", key="key")


@pytest.fixture
def entity_identifier_with_different_namespace():
    return EntityIdentifier(namespace="different_namespace", table="table", key="key")


@pytest.fixture
def entity_identifier_with_different_table():
    return EntityIdentifier(namespace="namespace", table="different_table", key="key")


@pytest.fixture
def entity_identifier_with_different_key():
    return EntityIdentifier(namespace="namespace", table="table", key="different_key")


# noinspection PyClassHasNoInit,PyShadowingNames,PyMethodMayBeStatic
class TestEntityIdentifier:

    def test_not_equal_if_namespace_is_different(self, entity_identifier, entity_identifier_with_different_namespace):
        self.assert_different(entity_identifier, entity_identifier_with_different_namespace)

    def test_not_equal_if_table_is_different(self, entity_identifier, entity_identifier_with_different_table):
        self.assert_different(entity_identifier, entity_identifier_with_different_table)

    def test_not_equal_if_key_is_different(self, entity_identifier, entity_identifier_with_different_key):
        self.assert_different(entity_identifier, entity_identifier_with_different_key)

    def test_equal_if_identical(self, entity_identifier, identical_entity_identifier):
        assert entity_identifier == identical_entity_identifier
        assert not (entity_identifier != identical_entity_identifier)
        assert hash(entity_identifier) == hash(identical_entity_identifier)

    def assert_different(self, entity, different):
        assert entity != different
        assert not (entity == different)
        assert hash(entity) != hash(different)
