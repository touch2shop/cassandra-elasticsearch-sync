from time import time
from uuid import uuid4

from hamcrest import assert_that, contains_inanyorder

from app.core.model.document import Document
from app.core.model.field import Field
from app.core.model.identifier import Identifier



# noinspection PyClassHasNoInit,PyMethodMayBeStatic
class TestDocument:

    def test_set_fields(self):
        identifier = Identifier(namespace="test", table="test", key=uuid4())
        document = Document(identifier, timestamp=time(), fields=[Field("foo", "bar")])
        assert document.fields == [Field("foo", "bar")]
        document.fields = [Field("one", 1), Field("two", 2)]
        assert_that(document.fields, contains_inanyorder(Field("one", 1), Field("two", 2)))

    def test_get_field_value(self):
        identifier = Identifier(namespace="test", table="test", key=uuid4())
        document = Document(identifier, timestamp=time(), fields=None)
        document.add_field("one", 1)
        document.add_field("two", 2)
        document.add_field("three", 3)
        assert document.get_field_value("one") is 1
        assert document.get_field_value("two") is 2
        assert document.get_field_value("three") is 3

    def test_set_field_value(self):
        identifier = Identifier(namespace="test", table="test", key=uuid4())
        document = Document(identifier, timestamp=time(), fields=None)
        document.fields = [Field("one", 1), Field("two", 3)]
        document.set_field_value("one", 3)
        document.set_field_value("two", 5)
        assert_that(document.fields, contains_inanyorder(Field("one", 3), Field("two", 5)))

    def test_add_field(self):
        identifier = Identifier(namespace="test", table="test", key=uuid4())
        document = Document(identifier, timestamp=time(), fields=None)
        document.fields = [Field("one", 1)]
        document.add_field("two", 2)
        document.add_field("three", 3)
        assert_that(document.fields, contains_inanyorder(Field("one", 1), Field("two", 2), Field("three", 3)))
