import unittest
import unittest.mock
import logging

from nervix.mainloop import Mainloop
from nervix.protocols.nxtcp import NxtcpConnection

from tests.util.sysmock import Sysmock, TcpAddress, patch
import tests.nxtcp_packet_definition as packets

logging.basicConfig(
    level=logging.INFO,
    # stream=sys.stdout,
    format="%(asctime)s %(levelname)s %(message)s"
)

CLIENT = TcpAddress('clienthost', 9999)
SERVER = TcpAddress('serverhost', 8888)


class Test(unittest.TestCase):

    def test_connect_1(self):
        """ Test if the connection will attempt to make an connection.
        """

        mock = Sysmock()
        mock.system.add_unused_local_address(CLIENT)

        with patch(mock):
            loop = Mainloop()

            mock.expect_tcp_syn(CLIENT, SERVER)
            mock.expect_sleep(3.0)

            conn = NxtcpConnection(loop, SERVER.address)

            mock.run_events(loop.run_once)

    def test_connect_retry_1(self):
        """ Test if the connection will retry making an connection if the first attempt doesn't result in a success
        within the timeout period.
        """

        mock = Sysmock()
        mock.system.add_unused_local_address(CLIENT)
        mock.system.add_unused_local_address(CLIENT)

        with patch(mock):
            loop = Mainloop()

            mock.expect_tcp_syn(CLIENT, SERVER)
            mock.expect_sleep(3.0)
            mock.expect_sleep(5.0)
            mock.expect_tcp_syn(CLIENT, SERVER)

            conn = NxtcpConnection(loop, SERVER.address)

            mock.run_events(loop.run_once)

    def test_connection_ready_1(self):
        """ Test if the connection will NOT yet be ready after the connection is set up, but the welcome packet has
        not yet been received.
        """

        mock = Sysmock()
        mock.system.add_unused_local_address(CLIENT)

        ready_handler = unittest.mock.Mock()

        with patch(mock):
            loop = Mainloop()

            mock.expect_tcp_syn(CLIENT, SERVER)
            mock.do_tcp_syn_ack(SERVER, CLIENT)

            conn = NxtcpConnection(loop, SERVER.address)
            conn.set_ready_handler(ready_handler)

            ready_handler.assert_called_once_with(False)
            ready_handler.reset_mock()

            mock.run_events(loop.run_once)

            ready_handler.assert_not_called()

    def test_connection_ready_2(self):
        """ Test if the connection will be ready after the connection is set up and the welcome packet has been
        received.
        """

        mock = Sysmock()
        mock.system.add_unused_local_address(CLIENT)

        ready_handler = unittest.mock.Mock()

        with patch(mock):
            loop = Mainloop()

            mock.expect_tcp_syn(CLIENT, SERVER)
            mock.do_tcp_syn_ack(SERVER, CLIENT)
            mock.do_tcp_input(SERVER, CLIENT, packets.welcome())

            conn = NxtcpConnection(loop, SERVER.address)
            conn.set_ready_handler(ready_handler)

            ready_handler.assert_called_once_with(False)
            ready_handler.reset_mock()

            mock.run_events(loop.run_once)

            ready_handler.assert_called_once_with(True)

    def test_connection_welcome_timeout(self):
        """ Test that the connection will not report ready when the welcome packet is send too late.
        """

        mock = Sysmock()
        mock.system.add_unused_local_address(CLIENT)

        ready_handler = unittest.mock.Mock()

        with patch(mock):
            loop = Mainloop()

            mock.expect_tcp_syn(CLIENT, SERVER)
            mock.do_tcp_syn_ack(SERVER, CLIENT)
            mock.expect_sleep(2.0)
            mock.expect_tcp_fin(CLIENT, SERVER)
            mock.do_tcp_input(SERVER, CLIENT, packets.welcome())

            conn = NxtcpConnection(loop, SERVER.address)
            conn.set_ready_handler(ready_handler)

            ready_handler.assert_called_once_with(False)
            ready_handler.reset_mock()

            mock.run_events(loop.run_once)

            ready_handler.assert_not_called()

    def test_disconnect_1(self):
        """ Test if the connection's ready status will be removed when the connection is closed on the server's end.
        """

        mock = Sysmock()
        mock.system.add_unused_local_address(CLIENT)

        ready_handler = unittest.mock.Mock()

        with patch(mock):
            loop = Mainloop()

            mock.expect_tcp_syn(CLIENT, SERVER)
            mock.do_tcp_syn_ack(SERVER, CLIENT)
            mock.do_tcp_input(SERVER, CLIENT, packets.welcome())

            conn = NxtcpConnection(loop, SERVER.address)
            conn.set_ready_handler(ready_handler)

            ready_handler.assert_called_once_with(False)
            ready_handler.reset_mock()

            mock.run_events(loop.run_once)

            ready_handler.assert_called_once_with(True)
            ready_handler.reset_mock()

            # now let the server close the connection and check if the ready_handler will be called with False

            mock.do_tcp_fin(SERVER, CLIENT)
            mock.expect_tcp_fin(CLIENT, SERVER)

            mock.run_events(loop.run_once)

            ready_handler.assert_called_once_with(False)

    def test_disconnect_2(self):
        """ Test if the connection's ready status will be removed when the connection is closed on the server's end.
        """

        mock = Sysmock()
        mock.system.add_unused_local_address(CLIENT)

        ready_handler = unittest.mock.Mock()

        with patch(mock):
            loop = Mainloop()

            mock.expect_tcp_syn(CLIENT, SERVER)
            mock.do_tcp_syn_ack(SERVER, CLIENT)

            conn = NxtcpConnection(loop, SERVER.address)
            conn.set_ready_handler(ready_handler)

            mock.run_events(loop.run_once)

            ready_handler.assert_called_once_with(False)
            ready_handler.reset_mock()

            # now let the server close the connection and check if the ready_handler will be called with False

            mock.do_tcp_fin(SERVER, CLIENT)
            mock.expect_tcp_fin(CLIENT, SERVER)

            mock.run_events(loop.run_once)

            ready_handler.assert_not_called()

    def test_disconnect_reconnect_1(self):
        """ Test if the connection will retry connecting after it was disconnected from the server.
        """

        mock = Sysmock()
        mock.system.add_unused_local_address(CLIENT)
        mock.system.add_unused_local_address(CLIENT)

        ready_handler = unittest.mock.Mock()

        with patch(mock):
            loop = Mainloop()

            mock.expect_tcp_syn(CLIENT, SERVER)
            mock.do_tcp_syn_ack(SERVER, CLIENT)
            mock.do_tcp_input(SERVER, CLIENT, packets.welcome())

            conn = NxtcpConnection(loop, SERVER.address)
            conn.set_ready_handler(ready_handler)

            ready_handler.assert_called_once_with(False)
            ready_handler.reset_mock()

            mock.run_events(loop.run_once)

            ready_handler.assert_called_once_with(True)
            ready_handler.reset_mock()

            # now close connection from server

            mock.do_tcp_fin(SERVER, CLIENT)
            mock.expect_tcp_fin(CLIENT, SERVER)

            mock.run_events(loop.run_once)

            ready_handler.assert_called_once_with(False)
            ready_handler.reset_mock()

            # now the connection should retry after 5 seconds
            mock.expect_sleep(5.0)
            mock.expect_tcp_syn(CLIENT, SERVER)

            mock.run_events(loop.run_once)

            # when this the connection is ready the callback should be called again
            mock.do_tcp_syn_ack(SERVER, CLIENT)
            mock.do_tcp_input(SERVER, CLIENT, packets.welcome())

            mock.run_events(loop.run_once)

            ready_handler.assert_called_once_with(True)

