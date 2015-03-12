from time import time
from uuid import uuid4
from hamcrest import assert_that, contains_inanyorder
from app.core.Identifier import Identifier
from app.core.ValueField import ValueField
from app.elasticsearch_domain.GenericElasticsearchDocument import GenericElasticsearchDocument


# noinspection PyClassHasNoInit,PyMethodMayBeStatic
class TestGenericElasticsearchDocument:

    def test_set_fields(self):
        identifier = Identifier(namespace="test", table="test", key=uuid4())
        document = GenericElasticsearchDocument(identifier, timestamp=time(), fields=[ValueField("foo", "bar")])
        assert document.fields == [ValueField("foo", "bar")]
        document.fields = [ValueField("one", 1), ValueField("two", 2)]
        assert_that(document.fields, contains_inanyorder(ValueField("one", 1), ValueField("two", 2)))

    def test_get_field_value(self):
        identifier = Identifier(namespace="test", table="test", key=uuid4())
        document = GenericElasticsearchDocument(identifier, timestamp=time(), fields=None)
        document.set_field_value("one", 1)
        document.set_field_value("two", 2)
        document.set_field_value("three", 3)
        assert document.get_field_value("one") is 1
        assert document.get_field_value("two") is 2
        assert document.get_field_value("three") is 3

    def test_set_field_value(self):
        identifier = Identifier(namespace="test", table="test", key=uuid4())
        document = GenericElasticsearchDocument(identifier, timestamp=time(), fields=None)
        document.fields = [ValueField("one", 1)]
        document.set_field_value("one", 3)
        document.set_field_value("two", 5)
        assert_that(document.fields, contains_inanyorder(ValueField("one", 3), ValueField("two", 5)))
