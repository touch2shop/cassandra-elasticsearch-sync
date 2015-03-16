from time import time
from uuid import uuid4

from hamcrest import assert_that, contains_inanyorder

from app.core.identifier import Identifier
from app.core.update_field import UpdateField
from app.core.generic_entity import GenericEntity


# noinspection PyClassHasNoInit,PyMethodMayBeStatic
class TestGenericEntity:

    def test_set_fields(self):
        identifier = Identifier(namespace="test", table="test", key=uuid4())
        document = GenericEntity(identifier, timestamp=time(), fields=[UpdateField("foo", "bar")])
        assert document.fields == [UpdateField("foo", "bar")]
        document.fields = [UpdateField("one", 1), UpdateField("two", 2)]
        assert_that(document.fields, contains_inanyorder(UpdateField("one", 1), UpdateField("two", 2)))

    def test_get_field_value(self):
        identifier = Identifier(namespace="test", table="test", key=uuid4())
        document = GenericEntity(identifier, timestamp=time(), fields=None)
        document.set_field_value("one", 1)
        document.set_field_value("two", 2)
        document.set_field_value("three", 3)
        assert document.get_field_value("one") is 1
        assert document.get_field_value("two") is 2
        assert document.get_field_value("three") is 3

    def test_set_field_value(self):
        identifier = Identifier(namespace="test", table="test", key=uuid4())
        document = GenericEntity(identifier, timestamp=time(), fields=None)
        document.fields = [UpdateField("one", 1)]
        document.set_field_value("one", 3)
        document.set_field_value("two", 5)
        assert_that(document.fields, contains_inanyorder(UpdateField("one", 3), UpdateField("two", 5)))
