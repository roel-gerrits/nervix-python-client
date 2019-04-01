import unittest

from nervix.serializers.string import StringSerializer


class Test(unittest.TestCase):

    def test_encode_string_1(self):
        s = StringSerializer()
        res = s.encode("test string")
        self.assertEqual(res, b"test string")

    def test_encode_bytes(self):
        s = StringSerializer()
        res = s.encode(b"test string")
        self.assertEqual(res, b"test string")

    def test_encode_object(self):
        class Example:
            def __str__(self):
                return "example"

        s = StringSerializer()
        res = s.encode(Example())
        self.assertEqual(res, b"example")

    def test_decode_1(self):
        s = StringSerializer()
        res = s.decode(b"test string")
        self.assertEqual(res, "test string")

    def test_decode_invalid_char_1(self):
        s = StringSerializer()
        res = s.decode(b"test string\xfe")
        self.assertEqual(res, "test string\\xfe")
