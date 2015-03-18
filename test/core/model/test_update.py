from time import time
from uuid import uuid4
from hamcrest import assert_that, contains_inanyorder, contains
import pytest
from app.core.model.document import Document

from app.core.model.identifier import Identifier
from app.core.model.update import Update
from app.core.model.field import Field


@pytest.fixture
def identifier():
    return Identifier("shop", "product", 123)


# noinspection PyShadowingNames
@pytest.fixture
def update(identifier):
    return Update(identifier=identifier, timestamp=time(), is_delete=False,
                  fields=[Field("a", 5), Field("b", "foo"), Field("c", "bar")])


# noinspection PyShadowingNames
@pytest.fixture
def identical_update(update):
    return Update(identifier=update.identifier, timestamp=update.timestamp, is_delete=update.is_delete,
                  fields=update.fields)


# noinspection PyShadowingNames
@pytest.fixture
def slightly_different_update(update):
    return Update(identifier=update.identifier, timestamp=time(), is_delete=False,
                  fields=[Field("a", 5), Field("b", "foo")])


@pytest.fixture
def very_different_update():
    different_identifier = Identifier("shop", "product", 456)
    return Update(identifier=different_identifier, timestamp=time(), is_delete=True,
                  fields=[Field("a", 5), Field("b", "foo")])


# noinspection PyMethodMayBeStatic,PyShadowingNames,PyClassHasNoInit
class TestUpdate:

    def test_equal_to_identical_update(self, update, identical_update):
        assert update == identical_update
        assert not (update != identical_update)
        assert hash(update) == hash(identical_update)

    def test_different_to_slightly_different_update(self, update, slightly_different_update):
        assert update != slightly_different_update
        assert not (update == slightly_different_update)
        assert hash(update) != hash(slightly_different_update)

    def test_different_to_very_different_update(self, update, very_different_update):
        assert update != very_different_update
        assert not (update == very_different_update)
        assert hash(update) != hash(very_different_update)

    def test_set_fields(self):
        identifier = Identifier(namespace="test", table="test", key=uuid4())
        document = Update(identifier, timestamp=time(), fields=[Field("foo", "bar")])
        assert_that(document.fields, contains(Field("foo", "bar")))
        document.fields = [Field("one", 1), Field("two", 2)]
        assert_that(document.fields, contains_inanyorder(Field("one", 1), Field("two", 2)))

    def test_create_update_from_document(self, identifier):
        document = Document(identifier, time(), fields=[Field("a", 2), Field("b", 2), Field("c", 3)])
        update = Update.from_document(document, is_delete=True)
        assert update.identifier == document.identifier
        assert update.timestamp == document.timestamp
        assert update.fields == document.fields
