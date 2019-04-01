from .base import BaseSerializer


class StringSerializer(BaseSerializer):

    def encode(self, obj):

        if isinstance(obj, bytes):
            return obj

        if not isinstance(obj, str):
            obj = str(obj)

        return obj.encode()

    def decode(self, bts):
        return bts.decode(errors='backslashreplace')
