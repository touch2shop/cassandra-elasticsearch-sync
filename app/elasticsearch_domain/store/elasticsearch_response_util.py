from app.core.model.identifier import Identifier
from app.util.timestamp_util import TimestampUtil


# noinspection PyClassHasNoInit
class ElasticsearchResponseUtil:

    @classmethod
    def extract_timestamp(cls, response):
        if "fields" in response and "_timestamp" in response["fields"]:
            _timestamp = response["fields"]["_timestamp"]
            if _timestamp:
                return TimestampUtil.milliseconds_to_seconds(_timestamp)
        return None

    @classmethod
    def extract_identifier(cls, response):
        _id = response["_id"]
        _type = response["_type"]
        _index = response["_index"]
        return Identifier(_index, _type, _id)

    @classmethod
    def extract_source(cls, response):
        return response["_source"]
