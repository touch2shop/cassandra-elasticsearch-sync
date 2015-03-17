from abc import abstractmethod
import abc

from elasticsearch import TransportError

from app.core.util.timestamp_util import TimestampUtil
from app.elasticsearch_domain.store.elasticsearch_response_util import ElasticsearchResponseUtil


class AbstractElasticsearchStore(object):

    __metaclass__ = abc.ABCMeta

    def __init__(self, elasticsearch_client):
        self._client = elasticsearch_client

    def _base_read(self, index, _type, _id):
        try:
            response = self._client.get(index=index, doc_type=_type, id=_id, _source=True, fields="_timestamp")
            return self._process_response(response)
        except TransportError as e:
            if e.status_code == 404:
                return None
            else:
                raise

    def _base_create(self, index, _type, _id, document):
        timestamp = self.__get_timestamp(document)
        body = self._to_request_body(document)
        self._client.create(index=index, doc_type=_type, id=_id, body=body, timestamp=timestamp, refresh=True)

    def _base_update(self, index, _type, _id, document):
        timestamp = self.__get_timestamp(document)
        body = self._to_request_body(document)
        self._client.update(index=index, doc_type=_type, id=_id, body={"doc": body}, timestamp=timestamp, refresh=True)

    def _base_delete(self, index, _type, _id):
        return self._client.delete(index=index, doc_type=_type, id=_id, refresh=True)

    @abstractmethod
    def _to_request_body(self, document):
        pass

    @abstractmethod
    def _from_response(self, source, timestamp, identifier):
        pass

    def _process_response(self, response):
        if not response["found"]:
            return None

        identifier = ElasticsearchResponseUtil.extract_identifier(response)
        timestamp = ElasticsearchResponseUtil.extract_timestamp(response)
        source = ElasticsearchResponseUtil.extract_source(response)
        return self._from_response(source, timestamp, identifier)

    @staticmethod
    def __get_timestamp(document):
        if document.timestamp:
            return TimestampUtil.seconds_to_milliseconds(document.timestamp)
        else:
            return None
