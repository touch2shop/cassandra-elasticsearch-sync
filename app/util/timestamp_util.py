# noinspection PyClassHasNoInit
class TimestampUtil:

    @classmethod
    def seconds_to_milliseconds(cls, timestamp):
        return int(round(timestamp * 1000))

    @classmethod
    def milliseconds_to_seconds(cls, timestamp):
        return timestamp / 1000.0

    @classmethod
    def are_equal_by_less_than(cls, left_timestamp, right_timestamp, difference):
        return abs(left_timestamp - right_timestamp) < difference
