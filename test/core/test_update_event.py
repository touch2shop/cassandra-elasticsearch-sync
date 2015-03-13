from time import sleep
import pytest
from time_uuid import TimeUUID
from app.core.identifier import Identifier
from app.core.update_event import UpdateEvent


def generate_timestamp():
    return TimeUUID.with_utcnow().get_timestamp()


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
def identical_update_event(update_event):
    return UpdateEvent(identifier=update_event.identifier, timestamp=update_event.timestamp,
                       is_delete=update_event.is_delete, field_names=update_event.field_names)


# noinspection PyShadowingNames
@pytest.fixture
def slightly_different_update_event(identifier):
    return UpdateEvent(identifier=identifier, timestamp=generate_timestamp(),
                       is_delete=False, field_names={"a", "b", "c"})


@pytest.fixture
def very_different_update_event():
    different_identifier = Identifier("shop", "product", 456)
    return UpdateEvent(identifier=different_identifier, timestamp=generate_timestamp(),
                       is_delete=True, field_names={})


# noinspection PyMethodMayBeStatic,PyShadowingNames,PyClassHasNoInit
class TestUpdateEvent:

    def test_equal_to_identical_update_event(self, update_event, identical_update_event):
        assert update_event == identical_update_event
        assert not (update_event != identical_update_event)
        assert hash(update_event) == hash(identical_update_event)

    def test_different_to_slightly_different_update_event(self, update_event, slightly_different_update_event):
        assert update_event != slightly_different_update_event
        assert not (update_event == slightly_different_update_event)
        assert hash(update_event) != hash(slightly_different_update_event)

    def test_different_to_very_different_update_event(self, update_event, very_different_update_event):
        assert update_event != very_different_update_event
        assert not (update_event == very_different_update_event)
        assert hash(update_event) != hash(very_different_update_event)

    def test_compare_by_timestamp_order_if_timestamps_are_different(self, identifier):
        before_timestamp = generate_timestamp()
        sleep(0.001)
        after_timestamp = generate_timestamp()

        before_event = UpdateEvent(identifier, before_timestamp)
        after_event = UpdateEvent(identifier, after_timestamp)

        assert cmp(before_event, after_event) < 0
        assert cmp(after_event, before_event) > 0

    def test_save_events_come_before_delete_events_if_timestamps_are_equal(self, identifier):
        timestamp = generate_timestamp()

        save_event = UpdateEvent(identifier, timestamp, is_delete=False)
        delete_event = UpdateEvent(identifier, timestamp, is_delete=True)

        assert cmp(save_event, delete_event) < 0
        assert cmp(delete_event, save_event) > 0

    def test_default_is_delete_is_false(self, identifier):
        timestamp = generate_timestamp()
        update = UpdateEvent(identifier=identifier, timestamp=timestamp)
        assert not update.is_delete
