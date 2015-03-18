from abc import abstractmethod, ABCMeta


class AbstractDocumentStore(object):

    __metaclass__ = ABCMeta

    @abstractmethod
    def read(self, identifier):
        pass

    @abstractmethod
    def create(self, document):
        pass

    @abstractmethod
    def update(self, document):
        pass

    @abstractmethod
    def delete(self, identifier):
        pass
