from app.elasticsearch_domain.store.AbstractElasticsearchStore import AbstractElasticsearchStore


class AbstractEntityElasticsearchStore(AbstractElasticsearchStore):

    def __init__(self, client, index, _type):
        super(AbstractEntityElasticsearchStore, self).__init__(client)
        self._index = index
        self._type = _type

    def read(self, _id):
        return self._base_read(self._index, self._type, _id)

    def delete(self, _id):
        self._base_delete(self._index, self._type, _id)

    def create(self, document):
        return self._base_create(self._index, self._type, document.id, document)

    def update(self, document):
        return self._base_update(self._index, self._type, document.id, document)
