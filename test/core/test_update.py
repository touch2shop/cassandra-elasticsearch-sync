import arrow
import pytest

from app.core.identifier import Identifier
from app.core.update import Update
from app.core.update_field import UpdateField


def generate_timestamp():
    return arrow.utcnow().float_timestamp


@pytest.fixture
def identifier():
    return Identifier("shop", "product", 123)


# noinspection PyShadowingNames
@pytest.fixture
def update(identifier):
    return Update(identifier=identifier, timestamp=generate_timestamp(), is_delete=False,
                  fields=[UpdateField("a", 5), UpdateField("b", "foo"), UpdateField("c", "bar")])


# noinspection PyShadowingNames
@pytest.fixture
def identical_update(update):
    return Update(identifier=update.identifier, timestamp=update.timestamp, is_delete=update.is_delete,
                  fields=update.fields)


# noinspection PyShadowingNames
@pytest.fixture
def slightly_different_update(update):
    return Update(identifier=update.identifier, timestamp=generate_timestamp(), is_delete=False,
                  fields=[UpdateField("a", 5), UpdateField("b", "foo")])


@pytest.fixture
def very_different_update():
    different_identifier = Identifier("shop", "product", 456)
    return Update(identifier=different_identifier, timestamp=generate_timestamp(), is_delete=True,
                  fields=[UpdateField("a", 5), UpdateField("b", "foo")])


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
