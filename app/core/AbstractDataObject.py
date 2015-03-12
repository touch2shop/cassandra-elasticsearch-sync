from abc import ABCMeta, abstractmethod


class AbstractDataObject(object):

    __metaclass__ = ABCMeta

    @abstractmethod
    def _deep_equals(self, other):
        pass

    def __eq__(self, other):
        if other is None:
            return False
        if not isinstance(other, self.__class__):
            return False
        return self._deep_equals(other)

    def __ne__(self, other):
        return not self.__eq__(other)

    @abstractmethod
    def _deep_hash(self):
        pass

    def __hash__(self):
        return self._deep_hash()

    def _deep_string_dictionary(self):
        return None

    def __repr__(self):
        deep_string_dictionary = self._deep_string_dictionary()
        if deep_string_dictionary:
            return repr(deep_string_dictionary)
        else:
            return super(AbstractDataObject, self).__repr__()

    def __str__(self):
        return repr(self)
