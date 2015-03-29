import arrow


# noinspection PyClassHasNoInit
class DateTimeUtil:

    @classmethod
    def are_equal_by_less_than(cls, left_datetime, right_datetime, seconds_difference):
        if left_datetime and right_datetime:
            return (arrow.get(left_datetime) - arrow.get(right_datetime)).total_seconds() < seconds_difference
        else:
            return left_datetime == right_datetime
