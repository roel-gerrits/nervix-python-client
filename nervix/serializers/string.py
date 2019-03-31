from .base import BaseSerializer


class StringSerializer(BaseSerializer):

    def encode(self, obj):
        if not isinstance(obj, str):
            obj = str(str)

        return obj.encode()

    def decode(self, bts):
        return bts.decode()
