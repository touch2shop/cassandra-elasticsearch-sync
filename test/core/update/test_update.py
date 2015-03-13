import arrow
import pytest

from app.core.identifier import Identifier
from app.core.update.event.update_event import UpdateEvent
from app.core.update.update import Update
from app.core.value_field import ValueField


def generate_timestamp():
    return arrow.utcnow().float_timestamp


@pytest.fixture
def identifier():
    return Identifier("shop", "product", 123)


# noinspection PyShadowingNames
@pytest.fixture
def update_event(identifier):
    return UpdateEvent(identifier=identifier, timestamp=generate_timestamp(),
                       is_delete=False, field_names={"a", "b", "c"})


# noinspection PyShadowingNames
@pytest.fixture
def update(update_event):
    return Update(event=update_event, fields=[ValueField("a", 5), ValueField("b", "foo"), ValueField("c", "bar")])


# noinspection PyShadowingNames
@pytest.fixture
def identical_update(update):
    return Update(event=update.event, fields=update.fields)


# noinspection PyShadowingNames
@pytest.fixture
def slightly_different_update(update_event):
    return Update(event=update_event,
                  fields=[ValueField("a", 5), ValueField("b", "foo")])


@pytest.fixture
def very_different_update():
    different_identifier = Identifier("shop", "product", 456)
    different_event = UpdateEvent(identifier=different_identifier, timestamp=generate_timestamp(),
                                  is_delete=False, field_names={"a", "b", "c"})
    return Update(event=different_event, fields=[ValueField("a", 5), ValueField("b", "foo")])


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

    def test_update_with_older_timestamp_has_lower_order_than_update_with_newer_timestamp(self, identifier):
        base_timestamp = generate_timestamp()

        older_update = Update(event=UpdateEvent(identifier=identifier, timestamp=base_timestamp,
                                                is_delete=False, field_names={"a", "b", "c"}))

        newer_update = Update(event=UpdateEvent(identifier=identifier, timestamp=base_timestamp + 0.001,
                                                is_delete=True, field_names={"d"}))

        assert older_update < newer_update
        assert older_update <= newer_update
        assert newer_update > older_update
        assert newer_update >= older_update

    def test_sort_by_timestamp_in_ascending_order(self):
        base_timestamp = generate_timestamp()

        update1 = Update(event=UpdateEvent(identifier=identifier, timestamp=base_timestamp + 1))
        update2 = Update(event=UpdateEvent(identifier=identifier, timestamp=base_timestamp + 2, field_names={"a", "b"}))
        update3 = Update(event=UpdateEvent(identifier=identifier, timestamp=base_timestamp + 2, field_names={"a"}))
        update4 = Update(event=UpdateEvent(identifier=identifier, timestamp=base_timestamp + 3))
        update5 = Update(event=UpdateEvent(identifier=identifier, timestamp=base_timestamp + 4))
        update6 = Update(event=UpdateEvent(identifier=identifier, timestamp=base_timestamp + 5))
        updates = [update6, update5, update2, update3, update1, update4]

        assert sorted(updates) == [update1, update2, update3, update4, update5, update6]