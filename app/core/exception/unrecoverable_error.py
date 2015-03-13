
class UnrecoverableError(Exception):

    def __init__(self, message):
        super(UnrecoverableError, self).__init__(message)
