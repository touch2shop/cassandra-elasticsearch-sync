# noinspection PyClassHasNoInit
class TimestampUtil:

    @classmethod
    def seconds_to_milliseconds(cls, timestamp):
        return int(round(timestamp * 1000))

    @classmethod
    def milliseconds_to_seconds(cls, timestamp):
        return timestamp / 1000.0
