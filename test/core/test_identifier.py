import pytest

from app.core.identifier import Identifier


@pytest.fixture
def identifier():
    return Identifier(namespace="namespace", table="table", key="key")


@pytest.fixture
def identical_identifier():
    return Identifier(namespace="namespace", table="table", key="key")


@pytest.fixture
def identifier_with_different_namespace():
    return Identifier(namespace="different_namespace", table="table", key="key")


@pytest.fixture
def identifier_with_different_table():
    return Identifier(namespace="namespace", table="different_table", key="key")


@pytest.fixture
def identifier_with_different_key():
    return Identifier(namespace="namespace", table="table", key="different_key")


# noinspection PyClassHasNoInit,PyShadowingNames,PyMethodMayBeStatic
class TestIdentifier:

    def test_not_equal_if_namespace_is_different(self, identifier, identifier_with_different_namespace):
        self.assert_different(identifier, identifier_with_different_namespace)

    def test_not_equal_if_table_is_different(self, identifier, identifier_with_different_table):
        self.assert_different(identifier, identifier_with_different_table)

    def test_not_equal_if_key_is_different(self, identifier, identifier_with_different_key):
        self.assert_different(identifier, identifier_with_different_key)

    def test_equal_if_identical(self, identifier, identical_identifier):
        assert identifier == identical_identifier
        assert not (identifier != identical_identifier)
        assert hash(identifier) == hash(identical_identifier)

    def assert_different(self, entity, different):
        assert entity != different
        assert not (entity == different)
        assert hash(entity) != hash(different)

    def test_to_string(self, identifier):
        assert repr(identifier) == str(identifier)
        assert repr(identifier) == "(%s, %s, %s)" % (identifier.namespace, identifier.table, identifier.key)
