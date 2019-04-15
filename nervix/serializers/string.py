from .base import BaseSerializer


class StringSerializer(BaseSerializer):
    """ Basic serializer.

    Encodes objects into bytes.
    Decodes bytes into UTF8 strings.
    """

    def encode(self, obj):

        if isinstance(obj, bytes):
            return obj

        if not isinstance(obj, str):
            obj = str(obj)

        return obj.encode()

    def decode(self, bts):
        return bts.decode(errors='backslashreplace')
