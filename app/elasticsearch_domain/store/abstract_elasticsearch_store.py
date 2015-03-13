from abc import abstractmethod
import abc
from datetime import datetime
from elasticsearch import TransportError


class AbstractElasticsearchStore(object):

    __metaclass__ = abc.ABCMeta

    def __init__(self, elasticsearch_client):
        self._client = elasticsearch_client

    def _base_read(self, index, _type, _id):
        try:
            response = self._client.get(index=index, doc_type=_type, id=_id, fields="_source,_timestamp")
            return self._process_response(response)
        except TransportError as e:
            if e.status_code == 404:
                return None
            else:
                raise

    def _base_create(self, index, _type, _id, document):
        time = self._get_time(document)
        body = self._to_request_body(document)
        self._client.create(index=index, doc_type=_type, id=_id, body=body, timestamp=time)

    def _base_update(self, index, _type, _id, document):
        time = self._get_time(document)
        body = self._to_request_body(document)
        self._client.update(index=index, doc_type=_type, id=_id, body={"doc": body}, timestamp=time)

    def _base_delete(self, index, _type, _id):
        return self._client.delete(index=index, doc_type=_type, id=_id)

    @abstractmethod
    def _to_request_body(self, document):
        pass

    @abstractmethod
    def _from_response(self, body, timestamp, index, _type, _id):
        pass

    def _process_response(self, response):
        if not response["found"]:
            return None

        _id = response["_id"]
        _type = response["_type"]
        _index = response["_index"]

        timestamp = self._extract_timestamp(response)
        return self._from_response(response["_source"], timestamp, _index, _type, _id)

    @staticmethod
    def _extract_timestamp(response):
        if "fields" in response and "_timestamp" in response["fields"]:
            return response["fields"]["_timestamp"] / 1000.0
        else:
            return None

    @staticmethod
    def _get_time(document):
        return datetime.utcfromtimestamp(document.timestamp) if document.timestamp else datetime.utcnow()
