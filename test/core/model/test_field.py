from decimal import Decimal
from time import sleep
from uuid import uuid4
from datetime import datetime

from app.core.model.field import Field


# noinspection PyMethodMayBeStatic,PyClassHasNoInit
class TestField:

    def test_string_field_values_are_identical(self):
        assert Field.field_values_are_identical("test 666", "test 666")
        assert Field.field_values_are_identical(unicode("test 666"), "test 666")
        assert Field.field_values_are_identical(unicode("test 666"), str("test 666"))

    def test_string_field_values_are_not_identical(self):
        assert not Field.field_values_are_identical("test 666", "test 777")

    def test_numeric_field_values_are_identical(self):
        assert Field.field_values_are_identical(15.35, 15.35)
        assert Field.field_values_are_identical(10, 10)
        assert Field.field_values_are_identical(10, 10.0)

    def test_numeric_field_values_are_not_identical(self):
        assert not Field.field_values_are_identical(15.35, 15.36)
        assert not Field.field_values_are_identical(10, 11)
        assert not Field.field_values_are_identical(10, 10.0001)

    def test_decimal_field_values_are_identical(self):
        assert Field.field_values_are_identical("15.35", Decimal("15.35"))
        assert Field.field_values_are_identical(Decimal("15.35"), Decimal("15.35"))

    def test_decimal_field_values_are_not_identical(self):
        assert not Field.field_values_are_identical("15.350", Decimal("15.35"))
        assert not Field.field_values_are_identical(Decimal("15.350"), Decimal("15.35"))

    def test_datetime_fields_are_identical(self):
        now = datetime.now()
        assert Field.field_values_are_identical(now, now)
        assert Field.field_values_are_identical(now, str(now))

        utc_now = datetime.utcnow()
        assert Field.field_values_are_identical(utc_now, utc_now)
        assert Field.field_values_are_identical(utc_now, str(utc_now))

    def test_datetime_fields_are_not_identical(self):
        time1 = datetime.now()
        sleep(0.001)
        time2 = datetime.now()

        assert not Field.field_values_are_identical(time1, time2)
        assert not Field.field_values_are_identical(time1, str(time2))

    def test_boolean_field_values_are_identical(self):
        assert Field.field_values_are_identical(True, True)
        assert Field.field_values_are_identical(False, False)

    def test_boolean_field_values_are_not_identical(self):
        assert not Field.field_values_are_identical(True, False)

    def test_uuid_field_values_are_identical(self):
        uuid = uuid4()
        assert Field.field_values_are_identical(uuid, uuid)
        assert Field.field_values_are_identical(uuid, str(uuid))
        assert Field.field_values_are_identical(uuid, unicode(uuid))

    def test_uuid_field_values_are_not_identical(self):
        uuid1 = uuid4()
        uuid2 = uuid4()
        assert not Field.field_values_are_identical(uuid1, uuid2)

    def test_fields_are_identical(self):
        job_id = uuid4()
        left_fields = [Field("name", "john doe"),
                       Field("salary", 1900.99),
                       Field("married", True),
                       Field("job_id", job_id)]

        right_fields = [Field("name", "john doe"),
                        Field("married", True),
                        Field("salary", 1900.99),
                        Field("points", 100),
                        Field("job_id", str(job_id))]

        assert Field.fields_are_identical(left_fields, right_fields)
        assert Field.fields_are_identical(right_fields, left_fields)

    def test_fields_are_not_identical(self):

        left_fields = [Field("name", "john doe"),
                       Field("salary", 1900.99),
                       Field("married", True),
                       Field("job_id", uuid4())]

        right_fields = [Field("name", "john doe"),
                        Field("married", True),
                        Field("salary", 1900.99),
                        Field("job_id", uuid4())]

        assert not Field.field_values_are_identical(left_fields, right_fields)
        assert not Field.field_values_are_identical(right_fields, left_fields)
