from abc import ABCMeta, abstractmethod


class AbstractUpdateApplier(object):

    __metaclass__ = ABCMeta

    @abstractmethod
    def apply_update(self, update):
        pass
