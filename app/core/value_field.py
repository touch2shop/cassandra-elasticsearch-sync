from datetime import datetime
from app.core.abstract_data_object import AbstractDataObject


class ValueField(AbstractDataObject):

    def __init__(self, name, value):
        self._name = name
        self._value = value

    @property
    def name(self):
        return self._name

    @property
    def value(self):
        return self._value

    def _deep_equals(self, other):
        return self.name == other.name and \
            self.value == other.value

    def _deep_hash(self):
        return hash((self.name, self.value))

    def _deep_string_dictionary(self):
        return {
            "name": self.name,
            "value": self.value
        }

    @classmethod
    def fields_are_identical(cls, left_fields, right_fields):
        for left_field in left_fields:
            for right_field in right_fields:
                if left_field.name.lower() == right_field.name.lower():
                    if not cls.field_values_are_identical(left_field.value, right_field.value):
                        return False

        return True

    @classmethod
    def field_values_are_identical(cls, left_field_value, right_field_value):
        if left_field_value is not None and right_field_value is not None:
            if cls.__value_is_numeric(left_field_value) and cls.__value_is_numeric(right_field_value):
                return left_field_value == right_field_value
            elif cls.__value_is_boolean(left_field_value) and cls.__value_is_boolean(right_field_value):
                return left_field_value == right_field_value
            elif cls.__value_is_datetime(left_field_value) and cls.__value_is_datetime(right_field_value):
                return left_field_value == right_field_value
            else:
                return str(left_field_value) == str(right_field_value)
        else:
            return left_field_value == right_field_value

    @classmethod
    def __value_is_numeric(cls, value):
        return isinstance(value, (int, long, float))

    @classmethod
    def __value_is_boolean(cls, value):
        return isinstance(value, bool)

    @classmethod
    def __value_is_datetime(cls, value):
        return isinstance(value, datetime)
