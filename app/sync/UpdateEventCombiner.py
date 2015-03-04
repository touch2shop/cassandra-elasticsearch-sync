from app.sync.CombinedUpdateEvent import CombinedUpdateEvent


class UpdateEventCombiner(object):

    @classmethod
    def __group_by_identifier(cls, update_events):
        grouped = dict()
        for update_event in update_events:
            identifier = update_event.identifier
            if identifier not in grouped:
                grouped[identifier] = list()
            grouped[identifier].append(update_event)
        return grouped

    @classmethod
    def combine(cls, update_events):
        events_by_identifier = cls.__group_by_identifier(update_events)

        combined_events = list()
        for identifier in events_by_identifier.keys():
            combined_event = CombinedUpdateEvent(identifier)
            for event in events_by_identifier[identifier]:
                combined_event.add(event)
            combined_events.append(combined_event)
        return combined_events
