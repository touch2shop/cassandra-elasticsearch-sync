import random
import uuid
from hamcrest import contains_inanyorder, assert_that
from app.sync.Identifier import Identifier
from app.sync.Update import Update
from app.sync.UpdateContainer import UpdateContainer
from app.sync.UpdateEvent import UpdateEvent


# noinspection PyMethodMayBeStatic,PyClassHasNoInit
class TestUpdateContainer:

    def test_updates_are_sorted_correctly(self):
        from_namespace1_table1 = [
            Update(UpdateEvent(Identifier("namespace1", "table1", uuid.uuid4()), timestamp=1)),
            Update(UpdateEvent(Identifier("namespace1", "table1", uuid.uuid4()), timestamp=2)),
            Update(UpdateEvent(Identifier("namespace1", "table1", uuid.uuid4()), timestamp=3)),
            Update(UpdateEvent(Identifier("namespace1", "table1", uuid.uuid4()), timestamp=4))
        ]

        from_namespace1_table2 = [
            Update(UpdateEvent(Identifier("namespace1", "table2", uuid.uuid4()), timestamp=1)),
            Update(UpdateEvent(Identifier("namespace1", "table2", uuid.uuid4()), timestamp=2)),
            Update(UpdateEvent(Identifier("namespace1", "table2", uuid.uuid4()), timestamp=3)),
            Update(UpdateEvent(Identifier("namespace1", "table2", uuid.uuid4()), timestamp=4))
        ]

        from_namespace2_table3 = [
            Update(UpdateEvent(Identifier("namespace2", "table3", uuid.uuid4()), timestamp=1)),
            Update(UpdateEvent(Identifier("namespace2", "table3", uuid.uuid4()), timestamp=2)),
            Update(UpdateEvent(Identifier("namespace2", "table3", uuid.uuid4()), timestamp=3)),
            Update(UpdateEvent(Identifier("namespace2", "table3", uuid.uuid4()), timestamp=4))
        ]

        from_namespace2_table4 = [
            Update(UpdateEvent(Identifier("namespace2", "table4", uuid.uuid4()), timestamp=1)),
            Update(UpdateEvent(Identifier("namespace2", "table4", uuid.uuid4()), timestamp=2)),
            Update(UpdateEvent(Identifier("namespace2", "table4", uuid.uuid4()), timestamp=3)),
            Update(UpdateEvent(Identifier("namespace2", "table4", uuid.uuid4()), timestamp=4))
        ]

        all_updates = list()
        all_updates.extend(from_namespace1_table1)
        all_updates.extend(from_namespace1_table2)
        all_updates.extend(from_namespace2_table3)
        all_updates.extend(from_namespace2_table4)
        random.shuffle(all_updates)

        container = UpdateContainer(all_updates)
        assert_that(container.get_namespaces(), contains_inanyorder("namespace1", "namespace2"))

        assert_that(container.get_tables("namespace1"), contains_inanyorder("table1", "table2"))
        assert_that(container.get_updates("namespace1", "table1"), contains_inanyorder(*from_namespace1_table1))
        assert_that(container.get_updates("namespace1", "table2"), contains_inanyorder(*from_namespace1_table2))

        assert_that(container.get_tables("namespace2"), contains_inanyorder("table3", "table4"))
        assert_that(container.get_updates("namespace2", "table3"), contains_inanyorder(*from_namespace2_table3))
        assert_that(container.get_updates("namespace2", "table4"), contains_inanyorder(*from_namespace2_table4))

        assert_that(container.get_all_updates(), contains_inanyorder(*all_updates))

    def test_can_iterate_over_updates(self):
        updates = [
            Update(UpdateEvent(Identifier("bla", "ble", uuid.uuid4()), timestamp=1)),
            Update(UpdateEvent(Identifier("bla", "ble", uuid.uuid4()), timestamp=2)),
            Update(UpdateEvent(Identifier("bla", "ble", uuid.uuid4()), timestamp=3)),
            Update(UpdateEvent(Identifier("bla", "ble", uuid.uuid4()), timestamp=4)),
        ]

        container = UpdateContainer(updates)

        iterated_updates = []
        for update in container:
            iterated_updates.append(update)

        assert len(iterated_updates) == len(updates)
        assert_that(iterated_updates, contains_inanyorder(*updates))

    def test_length(self):
        updates = [
            Update(UpdateEvent(Identifier("bla", "ble", uuid.uuid4()), timestamp=1)),
            Update(UpdateEvent(Identifier("bla", "ble", uuid.uuid4()), timestamp=2)),
            Update(UpdateEvent(Identifier("bla", "ble", uuid.uuid4()), timestamp=3)),
            Update(UpdateEvent(Identifier("bla", "ble", uuid.uuid4()), timestamp=4)),
        ]

        container = UpdateContainer(updates)
        assert len(container) == len(updates)
