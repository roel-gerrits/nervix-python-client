class EncodingError(RuntimeError):
    pass


class BaseSerializer:

    def encode(self, obj):
        """ Should return a bytes object that represents the given object.
        May raise an EncodingError in case of an error.
        """
        pass

    def decode(self, bts):
        """ Should decode the given bytes and return an object.
        Shouldn't raise exceptions when errors occur during decoding. Instead this should
        be logged, and the user should be presented with an object indicates the source of the
        error somehow.
        """
        pass
