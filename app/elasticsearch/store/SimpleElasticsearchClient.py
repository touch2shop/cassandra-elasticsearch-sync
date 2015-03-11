from elasticsearch.client import IndicesClient, Elasticsearch


class SimpleElasticsearchClient:

    def __init__(self, nodes):
        self._elasticsearch = Elasticsearch(nodes)
        self._indices_client = IndicesClient(self._elasticsearch)

    def index_exists(self, index):
        return self._indices_client.exists(index=index)

    def type_exists(self, index, doc_type):
        return self._indices_client.exists_type(index=index, doc_type=doc_type)

    def create_index(self, index):
        return self._indices_client.create(index=index)

    def delete_index(self, index):
        self._indices_client.delete(index=index)
