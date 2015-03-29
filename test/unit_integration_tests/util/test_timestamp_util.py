from app.util.timestamp_util import TimestampUtil


# noinspection PyMethodMayBeStatic,PyClassHasNoInit
class TestTimestampUtil:

    def test_seconds_to_milliseconds(self):
        assert TimestampUtil.seconds_to_milliseconds(10345.123456) == 10345123

    def test_milliseconds_to_seconds(self):
        assert TimestampUtil.milliseconds_to_seconds(10345123) == 10345.123
