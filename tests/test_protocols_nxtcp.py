import unittest
import unittest.mock
import logging

from nervix.mainloop import Mainloop
from nervix.protocols.nxtcp import NxtcpConnection
import nervix.verbs as verbs

from tests.util.sysmock import Sysmock, TcpAddress, patch
import tests.nxtcp_packet_definition as packets

logging.basicConfig(
    level=logging.DEBUG,
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

    def test_cooldown_timeouts(self):
        """ Test if the cooldown tieout increases in a certain pattern.
        """

        pattern = [5.0, 5.0, 5.0, 10.0, 10.0, 20.0, 30.0, 60.0, 60.0, 60.0, 60.0]

        mock = Sysmock()
        mock.system.add_unused_local_address(CLIENT)

        with patch(mock):
            loop = Mainloop()

            mock.expect_tcp_syn(CLIENT, SERVER)

            conn = NxtcpConnection(loop, SERVER.address)

            for cooldown in pattern:
                mock.expect_sleep(3.0)
                mock.expect_sleep(cooldown)
                mock.expect_tcp_syn(CLIENT, SERVER)

                mock.system.add_unused_local_address(CLIENT)
                mock.run_events(loop.run_once)

            # now let the connect succeed
            mock.do_tcp_syn_ack(SERVER, CLIENT)

            # and let the connection fail again
            mock.do_tcp_fin(SERVER, CLIENT)
            mock.expect_tcp_fin(CLIENT, SERVER)
            mock.system.add_unused_local_address(CLIENT)
            mock.run_events(loop.run_once)

            # now the next cooldown timeout should be the first one in the pattern again
            mock.expect_sleep(pattern[0])
            mock.expect_tcp_syn(CLIENT, SERVER)
            mock.system.add_unused_local_address(CLIENT)
            mock.run_events(loop.run_once)


def test_keepalive_1(self):
    """ Test if the connection will respond with a pong when the server sends a ping.
    """

    mock = Sysmock()
    mock.system.add_unused_local_address(CLIENT)

    with patch(mock):
        loop = Mainloop()

        mock.expect_tcp_syn(CLIENT, SERVER)
        mock.do_tcp_syn_ack(SERVER, CLIENT)
        mock.do_tcp_input(SERVER, CLIENT, packets.welcome())

        conn = NxtcpConnection(loop, SERVER.address)

        mock.expect_sleep(5.0)
        mock.do_tcp_input(SERVER, CLIENT, packets.ping())

        mock.expect_tcp_output(CLIENT, SERVER, packets.pong())

        mock.run_events(loop.run_once)


def test_byebye_1(self):
    """ Test that the client closes the connection when the server sends a byebye packet BEFORE the welcome packet.
    """

    mock = Sysmock()
    mock.system.add_unused_local_address(CLIENT)

    with patch(mock):
        loop = Mainloop()

        mock.expect_tcp_syn(CLIENT, SERVER)
        mock.do_tcp_syn_ack(SERVER, CLIENT)

        conn = NxtcpConnection(loop, SERVER.address)

        mock.run_events(loop.run_once)

        mock.do_tcp_input(SERVER, CLIENT, packets.byebye())

        mock.expect_tcp_fin(CLIENT, SERVER)

        mock.run_events(loop.run_once)


def test_byebye_2(self):
    """ Test that the client closes the connection when the server sends a byebye packet AFTER the welcome packet.
    """

    mock = Sysmock()
    mock.system.add_unused_local_address(CLIENT)

    with patch(mock):
        loop = Mainloop()

        mock.expect_tcp_syn(CLIENT, SERVER)
        mock.do_tcp_syn_ack(SERVER, CLIENT)
        mock.do_tcp_input(SERVER, CLIENT, packets.welcome())

        conn = NxtcpConnection(loop, SERVER.address)

        mock.run_events(loop.run_once)

        mock.do_tcp_input(SERVER, CLIENT, packets.byebye())

        mock.expect_tcp_fin(CLIENT, SERVER)

        mock.run_events(loop.run_once)


def test_session_1(self):
    """ Test that a Session verb is produced.
    """

    mock = Sysmock()
    mock.system.add_unused_local_address(CLIENT)

    downstream_handler = unittest.mock.Mock()

    with patch(mock):
        loop = Mainloop()

        mock.expect_tcp_syn(CLIENT, SERVER)
        mock.do_tcp_syn_ack(SERVER, CLIENT)
        mock.do_tcp_input(SERVER, CLIENT, packets.welcome())

        # send the packet
        mock.do_tcp_input(SERVER, CLIENT, packets.session(
            b'name',
            packets.SESSION_STATE_ACTIVE
        ))

        conn = NxtcpConnection(loop, SERVER.address)
        conn.set_downstream_handler(downstream_handler)

        mock.run_events(loop.run_once)

        # verify the verb
        downstream_handler.assert_called_once_with(verbs.SessionVerb(
            b'name',
            verbs.SessionVerb.STATE_ACTIVE
        ))


def test_session_2(self):
    """ Test that a Session verb is produced.
    """

    mock = Sysmock()
    mock.system.add_unused_local_address(CLIENT)

    downstream_handler = unittest.mock.Mock()

    with patch(mock):
        loop = Mainloop()

        mock.expect_tcp_syn(CLIENT, SERVER)
        mock.do_tcp_syn_ack(SERVER, CLIENT)
        mock.do_tcp_input(SERVER, CLIENT, packets.welcome())

        # send the packet
        mock.do_tcp_input(SERVER, CLIENT, packets.session(
            b'name',
            packets.SESSION_STATE_ENDED
        ))

        conn = NxtcpConnection(loop, SERVER.address)
        conn.set_downstream_handler(downstream_handler)

        mock.run_events(loop.run_once)

        # verify the verb
        downstream_handler.assert_called_once_with(verbs.SessionVerb(
            b'name',
            verbs.SessionVerb.STATE_ENDED
        ))


def test_session_3(self):
    """ Test that a Session verb is produced.
    """

    mock = Sysmock()
    mock.system.add_unused_local_address(CLIENT)

    downstream_handler = unittest.mock.Mock()

    with patch(mock):
        loop = Mainloop()

        mock.expect_tcp_syn(CLIENT, SERVER)
        mock.do_tcp_syn_ack(SERVER, CLIENT)
        mock.do_tcp_input(SERVER, CLIENT, packets.welcome())

        # send the packet
        mock.do_tcp_input(SERVER, CLIENT, packets.session(
            b'name',
            packets.SESSION_STATE_STANDBY
        ))

        conn = NxtcpConnection(loop, SERVER.address)
        conn.set_downstream_handler(downstream_handler)

        mock.run_events(loop.run_once)

        # verify the verb
        downstream_handler.assert_called_once_with(verbs.SessionVerb(
            b'name',
            verbs.SessionVerb.STATE_STANDBY
        ))


def test_call_1(self):
    """ Test that a Call verb is produced.
    """

    mock = Sysmock()
    mock.system.add_unused_local_address(CLIENT)

    downstream_handler = unittest.mock.Mock()

    with patch(mock):
        loop = Mainloop()

        mock.expect_tcp_syn(CLIENT, SERVER)
        mock.do_tcp_syn_ack(SERVER, CLIENT)
        mock.do_tcp_input(SERVER, CLIENT, packets.welcome())

        # send the packet
        mock.do_tcp_input(SERVER, CLIENT, packets.call(
            unidirectional=False,
            postref=1234,
            name=b'name',
            payload=b'payload',
        ))

        conn = NxtcpConnection(loop, SERVER.address)
        conn.set_downstream_handler(downstream_handler)

        mock.run_events(loop.run_once)

        # verify the verb
        downstream_handler.assert_called_once_with(verbs.CallVerb(
            unidirectional=False,
            postref=1234,
            name=b'name',
            payload=b'payload',
        ))


def test_call_2(self):
    """ Test that a Call verb is produced.
    """

    mock = Sysmock()
    mock.system.add_unused_local_address(CLIENT)

    downstream_handler = unittest.mock.Mock()

    with patch(mock):
        loop = Mainloop()

        mock.expect_tcp_syn(CLIENT, SERVER)
        mock.do_tcp_syn_ack(SERVER, CLIENT)
        mock.do_tcp_input(SERVER, CLIENT, packets.welcome())

        # send the packet
        mock.do_tcp_input(SERVER, CLIENT, packets.call(
            unidirectional=True,
            postref=1234,
            name=b'name',
            payload=b'payload',
        ))

        conn = NxtcpConnection(loop, SERVER.address)
        conn.set_downstream_handler(downstream_handler)

        mock.run_events(loop.run_once)

        # verify the verb
        downstream_handler.assert_called_once_with(verbs.CallVerb(
            unidirectional=True,
            postref=None,
            name=b'name',
            payload=b'payload',
        ))


def test_message_1(self):
    """ Test that a Message verb is produced.
    """

    mock = Sysmock()
    mock.system.add_unused_local_address(CLIENT)

    downstream_handler = unittest.mock.Mock()

    with patch(mock):
        loop = Mainloop()

        mock.expect_tcp_syn(CLIENT, SERVER)
        mock.do_tcp_syn_ack(SERVER, CLIENT)
        mock.do_tcp_input(SERVER, CLIENT, packets.welcome())

        # send the packet
        mock.do_tcp_input(SERVER, CLIENT, packets.message(
            messageref=1234,
            status=packets.MESSAGE_STATUS_OK,
            payload=b'payload',
        ))

        conn = NxtcpConnection(loop, SERVER.address)
        conn.set_downstream_handler(downstream_handler)

        mock.run_events(loop.run_once)

        # verify the verb
        downstream_handler.assert_called_once_with(verbs.MessageVerb(
            messageref=1234,
            status=verbs.MessageVerb.STATUS_OK,
            payload=b'payload',
        ))


def test_message_2(self):
    """ Test that a Message verb is produced.
    """

    mock = Sysmock()
    mock.system.add_unused_local_address(CLIENT)

    downstream_handler = unittest.mock.Mock()

    with patch(mock):
        loop = Mainloop()

        mock.expect_tcp_syn(CLIENT, SERVER)
        mock.do_tcp_syn_ack(SERVER, CLIENT)
        mock.do_tcp_input(SERVER, CLIENT, packets.welcome())

        # send the packet
        mock.do_tcp_input(SERVER, CLIENT, packets.message(
            messageref=1234,
            status=packets.MESSAGE_STATUS_TIMEOUT,
            payload=b'ignoreme',
        ))

        conn = NxtcpConnection(loop, SERVER.address)
        conn.set_downstream_handler(downstream_handler)

        mock.run_events(loop.run_once)

        # verify the verb
        downstream_handler.assert_called_once_with(verbs.MessageVerb(
            messageref=1234,
            status=verbs.MessageVerb.STATUS_TIMEOUT,
            payload=None,
        ))


def test_message_3(self):
    """ Test that a Message verb is produced.
    """

    mock = Sysmock()
    mock.system.add_unused_local_address(CLIENT)

    downstream_handler = unittest.mock.Mock()

    with patch(mock):
        loop = Mainloop()

        mock.expect_tcp_syn(CLIENT, SERVER)
        mock.do_tcp_syn_ack(SERVER, CLIENT)
        mock.do_tcp_input(SERVER, CLIENT, packets.welcome())

        # send the packet
        mock.do_tcp_input(SERVER, CLIENT, packets.message(
            messageref=1234,
            status=packets.MESSAGE_STATUS_UNREACHABLE,
            payload=b'ignoreme',
        ))

        conn = NxtcpConnection(loop, SERVER.address)
        conn.set_downstream_handler(downstream_handler)

        mock.run_events(loop.run_once)

        # verify the verb
        downstream_handler.assert_called_once_with(verbs.MessageVerb(
            messageref=1234,
            status=verbs.MessageVerb.STATUS_UNREACHABLE,
            payload=None,
        ))


def test_interest_1(self):
    """ Test that a Interest verb is produced.
    """

    mock = Sysmock()
    mock.system.add_unused_local_address(CLIENT)

    downstream_handler = unittest.mock.Mock()

    with patch(mock):
        loop = Mainloop()

        mock.expect_tcp_syn(CLIENT, SERVER)
        mock.do_tcp_syn_ack(SERVER, CLIENT)
        mock.do_tcp_input(SERVER, CLIENT, packets.welcome())

        # send the packet
        mock.do_tcp_input(SERVER, CLIENT, packets.interest(
            postref=1234,
            name=b'thename',
            status=packets.INTEREST_STATUS_INTEREST,
            topic=b'topic',
        ))

        conn = NxtcpConnection(loop, SERVER.address)
        conn.set_downstream_handler(downstream_handler)

        mock.run_events(loop.run_once)

        # verify the verb
        downstream_handler.assert_called_once_with(verbs.InterestVerb(
            postref=1234,
            name=b'thename',
            status=verbs.InterestVerb.STATUS_INTEREST,
            topic=b'topic',
        ))


def test_interest_2(self):
    """ Test that a Interest verb is produced.
    """

    mock = Sysmock()
    mock.system.add_unused_local_address(CLIENT)

    downstream_handler = unittest.mock.Mock()

    with patch(mock):
        loop = Mainloop()

        mock.expect_tcp_syn(CLIENT, SERVER)
        mock.do_tcp_syn_ack(SERVER, CLIENT)
        mock.do_tcp_input(SERVER, CLIENT, packets.welcome())

        # send the packet
        mock.do_tcp_input(SERVER, CLIENT, packets.interest(
            postref=1234,
            name=b'thename',
            status=packets.INTEREST_STATUS_NOINTEREST,
            topic=b'topic',
        ))

        conn = NxtcpConnection(loop, SERVER.address)
        conn.set_downstream_handler(downstream_handler)

        mock.run_events(loop.run_once)

        # verify the verb
        downstream_handler.assert_called_once_with(verbs.InterestVerb(
            postref=1234,
            name=b'thename',
            status=verbs.InterestVerb.STATUS_NO_INTEREST,
            topic=b'topic',
        ))


def test_login_1(self):
    """ Test that a login packet is send.
    """

    mock = Sysmock()
    mock.system.add_unused_local_address(CLIENT)

    with patch(mock):
        loop = Mainloop()

        mock.expect_tcp_syn(CLIENT, SERVER)
        mock.do_tcp_syn_ack(SERVER, CLIENT)
        mock.do_tcp_input(SERVER, CLIENT, packets.welcome())

        conn = NxtcpConnection(loop, SERVER.address)

        mock.run_events(loop.run_once)

        # the expected packet
        mock.expect_tcp_output(CLIENT, SERVER, packets.login(
            name=b'name',
            persist=False,
            standby=False,
            enforce=False,
        ))

        # put the verb upstream
        conn.send_verb(verbs.LoginVerb(
            name=b'name',
            persist=False,
            standby=False,
            enforce=False,
        ))

        mock.run_events(loop.run_once)


def test_login_2(self):
    """ Test that a login packet is send.
    """

    mock = Sysmock()
    mock.system.add_unused_local_address(CLIENT)

    with patch(mock):
        loop = Mainloop()

        mock.expect_tcp_syn(CLIENT, SERVER)
        mock.do_tcp_syn_ack(SERVER, CLIENT)
        mock.do_tcp_input(SERVER, CLIENT, packets.welcome())

        conn = NxtcpConnection(loop, SERVER.address)

        mock.run_events(loop.run_once)

        # the expected packet
        mock.expect_tcp_output(CLIENT, SERVER, packets.login(
            name=b'name',
            persist=True,
            standby=False,
            enforce=False,
        ))

        # put the verb upstream
        conn.send_verb(verbs.LoginVerb(
            name=b'name',
            persist=True,
            standby=False,
            enforce=False,
        ))

        mock.run_events(loop.run_once)


def test_login_3(self):
    """ Test that a login packet is send.
    """

    mock = Sysmock()
    mock.system.add_unused_local_address(CLIENT)

    with patch(mock):
        loop = Mainloop()

        mock.expect_tcp_syn(CLIENT, SERVER)
        mock.do_tcp_syn_ack(SERVER, CLIENT)
        mock.do_tcp_input(SERVER, CLIENT, packets.welcome())

        conn = NxtcpConnection(loop, SERVER.address)

        mock.run_events(loop.run_once)

        # the expected packet
        mock.expect_tcp_output(CLIENT, SERVER, packets.login(
            name=b'name',
            persist=False,
            standby=True,
            enforce=False,
        ))

        # put the verb upstream
        conn.send_verb(verbs.LoginVerb(
            name=b'name',
            persist=False,
            standby=True,
            enforce=False,
        ))

        mock.run_events(loop.run_once)


def test_login_4(self):
    """ Test that a login packet is send.
    """

    mock = Sysmock()
    mock.system.add_unused_local_address(CLIENT)

    with patch(mock):
        loop = Mainloop()

        mock.expect_tcp_syn(CLIENT, SERVER)
        mock.do_tcp_syn_ack(SERVER, CLIENT)
        mock.do_tcp_input(SERVER, CLIENT, packets.welcome())

        conn = NxtcpConnection(loop, SERVER.address)

        mock.run_events(loop.run_once)

        # the expected packet
        mock.expect_tcp_output(CLIENT, SERVER, packets.login(
            name=b'name',
            persist=False,
            standby=False,
            enforce=True,
        ))

        # put the verb upstream
        conn.send_verb(verbs.LoginVerb(
            name=b'name',
            persist=False,
            standby=False,
            enforce=True,
        ))

        mock.run_events(loop.run_once)


def test_logout_1(self):
    """ Test that a logout packet is send.
    """

    mock = Sysmock()
    mock.system.add_unused_local_address(CLIENT)

    with patch(mock):
        loop = Mainloop()

        mock.expect_tcp_syn(CLIENT, SERVER)
        mock.do_tcp_syn_ack(SERVER, CLIENT)
        mock.do_tcp_input(SERVER, CLIENT, packets.welcome())

        conn = NxtcpConnection(loop, SERVER.address)

        mock.run_events(loop.run_once)

        # the expected packet
        mock.expect_tcp_output(CLIENT, SERVER, packets.logout(
            name=b'name',
        ))

        # put the verb upstream
        conn.send_verb(verbs.LogoutVerb(
            name=b'name',
        ))

        mock.run_events(loop.run_once)


def test_request_1(self):
    """ Test that a request packet is send.
    """

    mock = Sysmock()
    mock.system.add_unused_local_address(CLIENT)

    with patch(mock):
        loop = Mainloop()

        mock.expect_tcp_syn(CLIENT, SERVER)
        mock.do_tcp_syn_ack(SERVER, CLIENT)
        mock.do_tcp_input(SERVER, CLIENT, packets.welcome())

        conn = NxtcpConnection(loop, SERVER.address)

        mock.run_events(loop.run_once)

        # the expected packet
        mock.expect_tcp_output(CLIENT, SERVER, packets.request(
            name=b'name',
            unidirectional=False,
            messageref=1234,
            timeout_ms=1000,
            payload=b'payload',
        ))

        # put the verb upstream
        conn.send_verb(verbs.RequestVerb(
            name=b'name',
            unidirectional=False,
            messageref=1234,
            timeout=1.0,
            payload=b'payload',
        ))

        mock.run_events(loop.run_once)


def test_request_2(self):
    """ Test that a request packet is send.
    """

    mock = Sysmock()
    mock.system.add_unused_local_address(CLIENT)

    with patch(mock):
        loop = Mainloop()

        mock.expect_tcp_syn(CLIENT, SERVER)
        mock.do_tcp_syn_ack(SERVER, CLIENT)
        mock.do_tcp_input(SERVER, CLIENT, packets.welcome())

        conn = NxtcpConnection(loop, SERVER.address)

        mock.run_events(loop.run_once)

        # the expected packet
        mock.expect_tcp_output(CLIENT, SERVER, packets.request(
            name=b'name',
            unidirectional=True,
            messageref=0,
            timeout_ms=0,
            payload=b'payload',
        ))

        # put the verb upstream
        conn.send_verb(verbs.RequestVerb(
            name=b'name',
            unidirectional=True,
            messageref=None,
            timeout=None,
            payload=b'payload',
        ))

        mock.run_events(loop.run_once)


def test_post_1(self):
    """ Test that a post packet is send.
    """

    mock = Sysmock()
    mock.system.add_unused_local_address(CLIENT)

    with patch(mock):
        loop = Mainloop()

        mock.expect_tcp_syn(CLIENT, SERVER)
        mock.do_tcp_syn_ack(SERVER, CLIENT)
        mock.do_tcp_input(SERVER, CLIENT, packets.welcome())

        conn = NxtcpConnection(loop, SERVER.address)

        mock.run_events(loop.run_once)

        # the expected packet
        mock.expect_tcp_output(CLIENT, SERVER, packets.post(
            postref=1234,
            payload=b'payload',
        ))

        # put the verb upstream
        conn.send_verb(verbs.PostVerb(
            postref=1234,
            payload=b'payload',
        ))

        mock.run_events(loop.run_once)


def test_subscribe_1(self):
    """ Test that a subscribe packet is send.
    """

    mock = Sysmock()
    mock.system.add_unused_local_address(CLIENT)

    with patch(mock):
        loop = Mainloop()

        mock.expect_tcp_syn(CLIENT, SERVER)
        mock.do_tcp_syn_ack(SERVER, CLIENT)
        mock.do_tcp_input(SERVER, CLIENT, packets.welcome())

        conn = NxtcpConnection(loop, SERVER.address)

        mock.run_events(loop.run_once)

        # the expected packet
        mock.expect_tcp_output(CLIENT, SERVER, packets.subscribe(
            messageref=1234,
            name=b'name',
            topic=b'topic',
        ))

        # put the verb upstream
        conn.send_verb(verbs.SubscribeVerb(
            messageref=1234,
            name=b'name',
            topic=b'topic',
        ))

        mock.run_events(loop.run_once)


def test_unsubscribe_1(self):
    """ Test that an unsubscribe packet is send.
    """

    mock = Sysmock()
    mock.system.add_unused_local_address(CLIENT)

    with patch(mock):
        loop = Mainloop()

        mock.expect_tcp_syn(CLIENT, SERVER)
        mock.do_tcp_syn_ack(SERVER, CLIENT)
        mock.do_tcp_input(SERVER, CLIENT, packets.welcome())

        conn = NxtcpConnection(loop, SERVER.address)

        mock.run_events(loop.run_once)

        # the expected packet
        mock.expect_tcp_output(CLIENT, SERVER, packets.unsubscribe(
            name=b'name',
            topic=b'topic',
        ))

        # put the verb upstream
        conn.send_verb(verbs.UnsubscribeVerb(
            name=b'name',
            topic=b'topic',
        ))

        mock.run_events(loop.run_once)
