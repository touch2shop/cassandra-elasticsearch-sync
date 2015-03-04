from hamcrest import *
import pytest

from app.sync.UpdateEvent import UpdateEvent
from app.sync.CombinedUpdateEvent import CombinedUpdateEvent
from app.sync.Identifier import Identifier


@pytest.fixture
def identifier():
    return Identifier("shop", "order", 123)


# noinspection PyClassHasNoInit,PyShadowingNames,PyMethodMayBeStatic
class TestCombinedUpdateEvent:

    def test_build_save_combined_event(self, identifier):
        timestamp = 1425324825.630933
        combined = CombinedUpdateEvent(identifier)
        combined.add(UpdateEvent(identifier, timestamp + 1, field_names={"a"}))
        combined.add(UpdateEvent(identifier, timestamp + 2, is_delete=True))
        combined.add(UpdateEvent(identifier, timestamp + 3, field_names={"b", "c"}))
        combined.add(UpdateEvent(identifier, timestamp + 4, field_names={"b", "c", "d"}))
        combined.add(UpdateEvent(identifier, timestamp + 5, field_names={"e", "f"}))

        assert combined.is_delete is False
        assert_that(combined.field_names, contains_inanyorder("a", "b", "c", "d", "e", "f"))
        assert combined.get_field("a").timestamp == timestamp + 1
        assert combined.get_field("b").timestamp == timestamp + 4
        assert combined.get_field("c").timestamp == timestamp + 4
        assert combined.get_field("d").timestamp == timestamp + 4
        assert combined.get_field("e").timestamp == timestamp + 5
        assert combined.get_field("f").timestamp == timestamp + 5

    def test_build_delete_combined_event(self, identifier):
        timestamp = 1425324825.630933
        combined = CombinedUpdateEvent(identifier)
        combined.add(UpdateEvent(identifier, timestamp + 1, field_names={"a"}))
        combined.add(UpdateEvent(identifier, timestamp + 6, is_delete=True))
        combined.add(UpdateEvent(identifier, timestamp + 3, field_names={"b", "c"}))
        combined.add(UpdateEvent(identifier, timestamp + 4, field_names={"b", "c", "d"}))
        combined.add(UpdateEvent(identifier, timestamp + 5, field_names={"e", "f"}))

        assert combined.is_delete is True
        assert combined.timestamp == timestamp + 6

    def test_when_adding_duplicate_fields_should_store_only_the_one_with_most_recent_timestamp(self, identifier):
        base_timestamp = 1425324825.630933
        combined = CombinedUpdateEvent(identifier)

        combined.add(UpdateEvent(identifier, base_timestamp + 1, field_names={"a"}))
        combined.add(UpdateEvent(identifier, base_timestamp + 5, is_delete=True))
        combined.add(UpdateEvent(identifier, base_timestamp + 4, field_names={"b", "c"}))
        combined.add(UpdateEvent(identifier, base_timestamp + 2, field_names={"b", "c", "d"}))
        combined.add(UpdateEvent(identifier, base_timestamp + 3, field_names={"e", "f"}))
        combined.add(UpdateEvent(identifier, base_timestamp + 6, field_names={"g"}))
        combined.add(UpdateEvent(identifier, base_timestamp + 7, field_names={"g"}))

        assert combined.get_field("a").timestamp == base_timestamp + 1
        assert combined.get_field("b").timestamp == base_timestamp + 4
        assert combined.get_field("c").timestamp == base_timestamp + 4
        assert combined.get_field("d").timestamp == base_timestamp + 2
        assert combined.get_field("e").timestamp == base_timestamp + 3
        assert combined.get_field("f").timestamp == base_timestamp + 3
        assert combined.get_field("g").timestamp == base_timestamp + 7

    def test_should_get_timestamp_from_most_recent_update(self, identifier):
        base_timestamp = 1425324825.630933

        combined = CombinedUpdateEvent(identifier)
        combined.add(UpdateEvent(identifier, timestamp=base_timestamp + 1, field_names={"a"}))
        combined.add(UpdateEvent(identifier, timestamp=base_timestamp + 5, is_delete=True))
        combined.add(UpdateEvent(identifier, timestamp=base_timestamp + 6, field_names={"g"}))
        combined.add(UpdateEvent(identifier, timestamp=base_timestamp + 3, field_names={"b", "c", "d"}))
        combined.add(UpdateEvent(identifier, timestamp=base_timestamp + 2, field_names={"b", "c"}))
        combined.add(UpdateEvent(identifier, timestamp=base_timestamp + 4, field_names={"e", "f"}))

        assert_that(combined.timestamp, equal_to(base_timestamp + 6))

    def test_is_delete_if_last_update_is_delete(self, identifier):
        base_timestamp = 1425324825.630933

        combined = CombinedUpdateEvent(identifier)
        combined.add(UpdateEvent(identifier, timestamp=base_timestamp + 1, field_names={"a"}))
        combined.add(UpdateEvent(identifier, timestamp=base_timestamp + 5, is_delete=True))
        combined.add(UpdateEvent(identifier, timestamp=base_timestamp + 5, field_names={"a"}))
        combined.add(UpdateEvent(identifier, timestamp=base_timestamp + 3, field_names={"b", "c", "d"}))
        combined.add(UpdateEvent(identifier, timestamp=base_timestamp + 2, field_names={"b", "c"}))
        combined.add(UpdateEvent(identifier, timestamp=base_timestamp + 4, field_names={"e", "f"}))

        assert_that(combined.is_delete, equal_to(True))

    def test_is_not_delete_if_last_update_is_save(self, identifier):
        base_timestamp = 1425324825.630933

        combined = CombinedUpdateEvent(identifier)
        combined.add(UpdateEvent(identifier, timestamp=base_timestamp + 1, field_names={"a"}))
        combined.add(UpdateEvent(identifier, timestamp=base_timestamp + 5, is_delete=True))
        combined.add(UpdateEvent(identifier, timestamp=base_timestamp + 5, field_names={"a"}))
        combined.add(UpdateEvent(identifier, timestamp=base_timestamp + 6, field_names={"a"}))
        combined.add(UpdateEvent(identifier, timestamp=base_timestamp + 3, field_names={"b", "c", "d"}))
        combined.add(UpdateEvent(identifier, timestamp=base_timestamp + 2, field_names={"b", "c"}))
        combined.add(UpdateEvent(identifier, timestamp=base_timestamp + 4, field_names={"e", "f"}))

        assert_that(combined.is_delete, equal_to(False))
