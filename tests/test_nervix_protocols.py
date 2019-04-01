import unittest

from nervix import create_connection
from nervix.mainloop import Mainloop
from nervix.protocols.base import host_port_parser
from nervix.protocols.nxtcp import NxtcpConnection


class Test(unittest.TestCase):

    def test_host_port_parser_1(self):
        address = host_port_parser("host:1234")
        self.assertEqual(address, ('host', 1234))

    def test_host_port_parser_2(self):
        address = host_port_parser(":1234")
        self.assertEqual(address, ('', 1234))

    def test_host_port_parser_3(self):
        with self.assertRaises(ValueError):
            host_port_parser("1234")

    def test_host_port_parser_4(self):
        with self.assertRaises(ValueError):
            host_port_parser(":")

    def test_create_connection_nxtcp_1(self):
        loop = Mainloop()
        conn = create_connection(loop, 'nxtcp://localhost:1234')
        self.assertIsInstance(conn, NxtcpConnection)
