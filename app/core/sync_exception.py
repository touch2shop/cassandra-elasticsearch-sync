
class SyncException(Exception):

    def __init__(self, message, identifier=None):
        if identifier:
            super_message = "Error syncing entity %s: %s" % (identifier, message)
        else:
            super_message = message
        super(SyncException, self).__init__(super_message)
        self._identifier = identifier

    @property
    def identifier(self):
        return self._identifier
