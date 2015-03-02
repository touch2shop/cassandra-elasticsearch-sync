from hamcrest import *
import pytest

from app.sync.UpdateEvent import UpdateEvent
from app.sync.CombinedUpdateEvent import CombinedUpdateEvent
from app.sync.EntityIdentifier import EntityIdentifier


@pytest.fixture
def identifier():
    return EntityIdentifier("shop", "order", 123)


# noinspection PyClassHasNoInit,PyShadowingNames,PyMethodMayBeStatic
class TestCombinedUpdateEvent:

    def test_get_field_names(self, identifier):
        timestamp = 1425324825.630933
        combined = CombinedUpdateEvent(identifier)
        combined.add_update_event(UpdateEvent(identifier, timestamp, field_names={"a"}))
        combined.add_update_event(UpdateEvent(identifier, timestamp, is_delete=True))
        combined.add_update_event(UpdateEvent(identifier, timestamp, field_names={"b", "c"}))
        combined.add_update_event(UpdateEvent(identifier, timestamp, field_names={"b", "c", "d"}))
        combined.add_update_event(UpdateEvent(identifier, timestamp, field_names={"e", "f"}))
        assert_that(combined.field_names, contains_inanyorder("a", "b", "c", "d", "e", "f"))

    def test_should_get_timestamp_from_most_recent_update(self, identifier):
        base_timestamp = 1425324825.630933

        combined = CombinedUpdateEvent(identifier)
        combined.add_update_event(UpdateEvent(identifier, timestamp=base_timestamp + 1, field_names={"a"}))
        combined.add_update_event(UpdateEvent(identifier, timestamp=base_timestamp + 5, is_delete=True))
        combined.add_update_event(UpdateEvent(identifier, timestamp=base_timestamp + 6, field_names={"g"}))
        combined.add_update_event(UpdateEvent(identifier, timestamp=base_timestamp + 3, field_names={"b", "c", "d"}))
        combined.add_update_event(UpdateEvent(identifier, timestamp=base_timestamp + 2, field_names={"b", "c"}))
        combined.add_update_event(UpdateEvent(identifier, timestamp=base_timestamp + 4, field_names={"e", "f"}))

        assert_that(combined.timestamp, equal_to(base_timestamp + 6))

    def test_is_delete_if_last_update_is_delete(self, identifier):
        base_timestamp = 1425324825.630933

        combined = CombinedUpdateEvent(identifier)
        combined.add_update_event(UpdateEvent(identifier, timestamp=base_timestamp + 1, field_names={"a"}))
        combined.add_update_event(UpdateEvent(identifier, timestamp=base_timestamp + 5, is_delete=True))
        combined.add_update_event(UpdateEvent(identifier, timestamp=base_timestamp + 5, field_names={"a"}))
        combined.add_update_event(UpdateEvent(identifier, timestamp=base_timestamp + 3, field_names={"b", "c", "d"}))
        combined.add_update_event(UpdateEvent(identifier, timestamp=base_timestamp + 2, field_names={"b", "c"}))
        combined.add_update_event(UpdateEvent(identifier, timestamp=base_timestamp + 4, field_names={"e", "f"}))

        assert_that(combined.is_delete, equal_to(True))

    def test_is_not_delete_if_last_update_is_save(self, identifier):
        base_timestamp = 1425324825.630933

        combined = CombinedUpdateEvent(identifier)
        combined.add_update_event(UpdateEvent(identifier, timestamp=base_timestamp + 1, field_names={"a"}))
        combined.add_update_event(UpdateEvent(identifier, timestamp=base_timestamp + 5, is_delete=True))
        combined.add_update_event(UpdateEvent(identifier, timestamp=base_timestamp + 5, field_names={"a"}))
        combined.add_update_event(UpdateEvent(identifier, timestamp=base_timestamp + 6, field_names={"a"}))
        combined.add_update_event(UpdateEvent(identifier, timestamp=base_timestamp + 3, field_names={"b", "c", "d"}))
        combined.add_update_event(UpdateEvent(identifier, timestamp=base_timestamp + 2, field_names={"b", "c"}))
        combined.add_update_event(UpdateEvent(identifier, timestamp=base_timestamp + 4, field_names={"e", "f"}))

        assert_that(combined.is_delete, equal_to(False))
