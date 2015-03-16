from uuid import uuid4
import arrow
from hamcrest import assert_that, contains_inanyorder, contains
import pytest

from app.core.identifier import Identifier
from app.core.update import Update
from app.core.field import Field


def generate_timestamp():
    return arrow.utcnow().float_timestamp


@pytest.fixture
def identifier():
    return Identifier("shop", "product", 123)


# noinspection PyShadowingNames
@pytest.fixture
def update(identifier):
    return Update(identifier=identifier, timestamp=generate_timestamp(), is_delete=False,
                  fields=[Field("a", 5), Field("b", "foo"), Field("c", "bar")])


# noinspection PyShadowingNames
@pytest.fixture
def identical_update(update):
    return Update(identifier=update.identifier, timestamp=update.timestamp, is_delete=update.is_delete,
                  fields=update.fields)


# noinspection PyShadowingNames
@pytest.fixture
def slightly_different_update(update):
    return Update(identifier=update.identifier, timestamp=generate_timestamp(), is_delete=False,
                  fields=[Field("a", 5), Field("b", "foo")])


@pytest.fixture
def very_different_update():
    different_identifier = Identifier("shop", "product", 456)
    return Update(identifier=different_identifier, timestamp=generate_timestamp(), is_delete=True,
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
        document = Update(identifier, timestamp=arrow.utcnow(), fields=[Field("foo", "bar")])
        assert_that(document.fields, contains(Field("foo", "bar")))
        document.fields = [Field("one", 1), Field("two", 2)]
        assert_that(document.fields, contains_inanyorder(Field("one", 1), Field("two", 2)))