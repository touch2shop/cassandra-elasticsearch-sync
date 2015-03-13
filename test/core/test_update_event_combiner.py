from hamcrest import assert_that, contains_inanyorder
from app.core.identifier import Identifier
from app.core.update_event import UpdateEvent
from app.core.update_event_combiner import UpdateEventCombiner


# noinspection PyMethodMayBeStatic,PyClassHasNoInit
class TestUpdateEventCombiner:

    def test_combine_update_events_by_identifier(self):
        base_timestamp = 1425324825.630933

        combined_events = [
            UpdateEvent(Identifier("test", "product", 1), base_timestamp + 1, field_names={"a", "b", "c"}),
            UpdateEvent(Identifier("test", "product", 1), base_timestamp + 2, field_names={"a", "b"}),
            UpdateEvent(Identifier("test", "product", 2), base_timestamp + 2, field_names={"c", "d"}),
            UpdateEvent(Identifier("test", "product", 2), base_timestamp + 4, field_names={"c", "d"}),
            UpdateEvent(Identifier("test", "product", 1), base_timestamp + 5, is_delete=True),
            UpdateEvent(Identifier("test", "product", 2), base_timestamp + 6, field_names={"e"}),
            UpdateEvent(Identifier("test", "product", 3), base_timestamp + 7, field_names={"e"})]

        combined_events = UpdateEventCombiner.combine(combined_events)
        combined_events_by_identifier = {}
        for combined_event in combined_events:
            identifier = combined_event.identifier
            assert identifier not in combined_events_by_identifier
            combined_events_by_identifier[identifier] = combined_event

        combined_event_1 = combined_events_by_identifier[Identifier("test", "product", 1)]
        assert combined_event_1.is_delete is True
        assert combined_event_1.timestamp == base_timestamp + 5

        combined_event_2 = combined_events_by_identifier[Identifier("test", "product", 2)]
        assert combined_event_2.is_delete is False
        assert_that(combined_event_2.field_names, contains_inanyorder("c", "d", "e"))
        assert combined_event_2.timestamp == base_timestamp + 6

        combined_event_3 = combined_events_by_identifier[Identifier("test", "product", 3)]
        assert combined_event_3.is_delete is False
        assert_that(combined_event_3.field_names, contains_inanyorder("e"))
        assert combined_event_3.timestamp == base_timestamp + 7
