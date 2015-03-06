from elasticsearch.client import IndicesClient


class SimpleElasticsearchClient:

    def __init__(self, elasticsearch):
        self._elasticsearch = elasticsearch
        self._indices_client = IndicesClient(elasticsearch)

    def index_exists(self, index):
        return self._indices_client.exists(index=index)

    def type_exists(self, index, doc_type):
        return self._indices_client.exists_type(index=index, doc_type=doc_type)
