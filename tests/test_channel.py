import unittest
from unittest.mock import Mock

from nervix.channel import Channel
from nervix.core import Message, Post, Interest, Call, MessageStatus, InterestStatus, HandlerList
from nervix import verbs

from tests.util.mockedconnection import MockedConnection
from tests.util.mockedtime import patch_time


class Test(unittest.TestCase):

    def test_subscribe_unsubscribe_1(self):
        """ Test if both an subscribe and unsubscribe verb is pushed when the
        connection is ready before creation of the subscription.
        """

        conn = MockedConnection()
        chan = Channel(conn)

        conn.mock_connection_ready(True)

        sub = chan.subscribe('name', 'topic')

        conn.assert_upstream_verb(verbs.SubscribeVerb(
            name=b'name',
            topic=b'topic',
            messageref=1,
        ))

        sub.cancel()

        conn.assert_upstream_verb(verbs.UnsubscribeVerb(
            name=b'name',
            topic=b'topic',
        ))

        conn.assert_upstream_verb(None)

    def test_subscribe_unsubscribe_2(self):
        """ Test if subscribe and unusbscribe verbs is pushed even when the connection only
        becomes ready after the subscription is created.
        """

        conn = MockedConnection()
        chan = Channel(conn)

        conn.mock_connection_ready(False)

        sub = chan.subscribe('name', 'topic')

        conn.mock_connection_ready(True)

        conn.assert_upstream_verb(verbs.SubscribeVerb(
            name=b'name',
            topic=b'topic',
            messageref=1,
        ))

        sub.cancel()

        conn.assert_upstream_verb(verbs.UnsubscribeVerb(
            name=b'name',
            topic=b'topic',
        ))

        conn.assert_upstream_verb(None)

    def test_subscribe_unsubscribe_3(self):
        """ Test if no verbs at all are pushed if the subscription is created and canceled before
        the connection was ready.
        """

        conn = MockedConnection()
        chan = Channel(conn)

        conn.mock_connection_ready(False)

        sub = chan.subscribe('name', 'topic')

        sub.cancel()

        conn.mock_connection_ready(True)

        conn.assert_upstream_verb(None)

    def test_login_logout_1(self):
        """ Test if the login and logout verbs are pushed when the sesison is created after the
        connection became ready.
        """

        conn = MockedConnection()
        chan = Channel(conn)

        conn.mock_connection_ready(True)

        session = chan.session('name')

        conn.assert_upstream_verb(verbs.LoginVerb(
            name=b'name',
            enforce=False,
            standby=False,
            persist=False,
        ))

        session.cancel()

        conn.assert_upstream_verb(verbs.LogoutVerb(
            name=b'name',
        ))

        conn.assert_upstream_verb(None)

    def test_login_logout_2(self):
        """ Test if both the login and logout verb are pushed when the connection becomes ready
        after the session is created.
        """

        conn = MockedConnection()
        chan = Channel(conn)

        conn.mock_connection_ready(False)

        session = chan.session('name')

        conn.mock_connection_ready(True)

        conn.assert_upstream_verb(verbs.LoginVerb(
            name=b'name',
            enforce=False,
            standby=False,
            persist=False,
        ))

        session.cancel()

        conn.assert_upstream_verb(verbs.LogoutVerb(
            name=b'name',
        ))

        conn.assert_upstream_verb(None)

    def test_login_logout_3(self):
        """ Test if no verbs are pushed at all if the session is created and canceled before the
        connection became ready.
        """

        conn = MockedConnection()
        chan = Channel(conn)

        conn.mock_connection_ready(False)

        session = chan.session('name')

        session.cancel()

        conn.mock_connection_ready(True)

        conn.assert_upstream_verb(None)

    def test_request_1(self):
        """ Test if a request verb is pushed when the connection is ready immediately.
        """

        conn = MockedConnection()
        chan = Channel(conn)

        conn.mock_connection_ready(True)

        chan.request('name', 'payload').send()

        conn.assert_upstream_verb(verbs.RequestVerb(
            name=b'name',
            unidirectional=True,
            messageref=None,
            timeout=5.0,
            payload=b'payload'
        ))

    def test_request_2(self):
        """ Test if the pushed request verb is unidirectional and has no messageref when no
        handlers were set.
        """

        conn = MockedConnection()
        chan = Channel(conn)

        conn.mock_connection_ready(True)

        req = chan.request('name', 'payload')
        req.send()

        conn.assert_upstream_verb(verbs.RequestVerb(
            name=b'name',
            unidirectional=True,
            messageref=None,
            timeout=5.0,
            payload=b'payload'
        ))

    def test_request_3(self):
        """ Test if the pushed request verb is directional and has a messsageref when a handler
        is set.
        """

        conn = MockedConnection()
        chan = Channel(conn)

        conn.mock_connection_ready(True)

        req = chan.request('name', 'payload')
        req.set_handler(Mock())
        req.send()

        conn.assert_upstream_verb(verbs.RequestVerb(
            name=b'name',
            unidirectional=False,
            messageref=1,
            timeout=5.0,
            payload=b'payload'
        ))

    def test_request_4(self):
        """ Test if a request is still pushed when the connection becomes ready just before
        the ttl is expires.
        """

        conn = MockedConnection()
        chan = Channel(conn)

        with patch_time() as time:
            conn.mock_connection_ready(False)

            chan.request('name', 'payload').send(ttl=5.0)

            time.sleep(4.999)

            conn.mock_connection_ready(True)

            conn.assert_upstream_verb(verbs.RequestVerb(
                name=b'name',
                unidirectional=True,
                messageref=None,
                timeout=5.0,
                payload=b'payload'
            ))

    def test_request_5(self):
        """ Test if a request is discarded when connection becomes ready after the ttl is expired.
        """

        conn = MockedConnection()
        chan = Channel(conn)

        conn.mock_connection_ready(False)

        with patch_time() as time:
            chan.request('name', 'payload').send(ttl=5.0)

            time.sleep(5.001)

            conn.mock_connection_ready(True)

        conn.assert_upstream_verb(None)

    def test_post_1(self):
        """ Test if a post verb is pushed when posted and connection was ready.
        """

        conn = MockedConnection()
        chan = Channel(conn)

        conn.mock_connection_ready(True)

        post = Post(chan.core, postref=1, payload='payload', ttl=5.0)

        conn.assert_upstream_verb(verbs.PostVerb(
            postref=1,
            payload=b'payload',
        ))

    def test_post_2(self):
        """ Test if a post verb is pushed when posted before connection was ready.
        """

        conn = MockedConnection()
        chan = Channel(conn)

        conn.mock_connection_ready(False)

        post = Post(chan.core, postref=1, payload='payload', ttl=5.0)

        conn.mock_connection_ready(True)

        conn.assert_upstream_verb(verbs.PostVerb(
            postref=1,
            payload=b'payload',
        ))

    def test_post_3(self):
        """ Test if a post is still pushed when the connection becomes ready just before
        the ttl is expires.
        """

        conn = MockedConnection()
        chan = Channel(conn)

        with patch_time() as time:
            conn.mock_connection_ready(False)

            post = Post(chan.core, postref=1, payload='payload', ttl=5.0)

            time.sleep(4.999)

            conn.mock_connection_ready(True)

            conn.assert_upstream_verb(verbs.PostVerb(
                postref=1,
                payload=b'payload',
            ))

    def test_post_4(self):
        """ Test if a post is discarded when connection becomes ready after the ttl is expired.
        """

        conn = MockedConnection()
        chan = Channel(conn)

        conn.mock_connection_ready(False)

        with patch_time() as time:
            chan.request('name', 'payload').send(ttl=5.0)

            time.sleep(5.001)

            conn.mock_connection_ready(True)

        conn.assert_upstream_verb(None)

    def test_backlog_order(self):
        """ Test if the order of requests is preserved when queued in the backlog.
        """

        conn = MockedConnection()
        chan = Channel(conn)

        conn.mock_connection_ready(False)

        chan.request('name', 'payload0').send(ttl=5.0)
        chan.request('name', 'payload1').send(ttl=5.0)
        chan.request('name', 'payload2').send(ttl=5.0)

        conn.mock_connection_ready(True)

        conn.assert_upstream_verb(verbs.RequestVerb(
            name=b'name',
            unidirectional=True,
            messageref=None,
            timeout=5.0,
            payload=b'payload0'
        ))

        conn.assert_upstream_verb(verbs.RequestVerb(
            name=b'name',
            unidirectional=True,
            messageref=None,
            timeout=5.0,
            payload=b'payload1'
        ))

        conn.assert_upstream_verb(verbs.RequestVerb(
            name=b'name',
            unidirectional=True,
            messageref=None,
            timeout=5.0,
            payload=b'payload2'
        ))

    def test_backlog_different_ttls(self):
        """ Test if the backlog discards requests whos ttl have expired but keeps
        others.
        """

        conn = MockedConnection()
        chan = Channel(conn)

        with patch_time() as time:
            conn.mock_connection_ready(False)

            chan.request('name', 'payload0').send(ttl=2.0)
            chan.request('name', 'payload1').send(ttl=4.0)
            chan.request('name', 'payload2').send(ttl=2.0)
            chan.request('name', 'payload3').send(ttl=4.0)

            time.sleep(3.0)

            conn.mock_connection_ready(True)

        conn.assert_upstream_verb(verbs.RequestVerb(
            name=b'name',
            unidirectional=True,
            messageref=None,
            timeout=5.0,
            payload=b'payload1'
        ))

        conn.assert_upstream_verb(verbs.RequestVerb(
            name=b'name',
            unidirectional=True,
            messageref=None,
            timeout=5.0,
            payload=b'payload3'
        ))

    def test_backlog_no_ttl(self):
        """ Test if the backlog discards requests with no ttl at all.
        """

        conn = MockedConnection()
        chan = Channel(conn)

        conn.mock_connection_ready(False)

        chan.request('name', 'payload0').send(ttl=0)
        chan.request('name', 'payload1').send(ttl=4.0)
        chan.request('name', 'payload2').send(ttl=0)
        chan.request('name', 'payload3').send(ttl=4.0)

        conn.mock_connection_ready(True)

        conn.assert_upstream_verb(verbs.RequestVerb(
            name=b'name',
            unidirectional=True,
            messageref=None,
            timeout=5.0,
            payload=b'payload1'
        ))

        conn.assert_upstream_verb(verbs.RequestVerb(
            name=b'name',
            unidirectional=True,
            messageref=None,
            timeout=5.0,
            payload=b'payload3'
        ))

    def test_handlerlist_message_ok(self):
        """ Test if the correct handler is called.
        """

        ok_handler = Mock()
        unreachable_handler = Mock()
        timeout_handler = Mock()
        not_ok_handler = Mock()
        any_handler = Mock()

        hl = HandlerList()
        hl.add(ok_handler, MessageStatus.OK)
        hl.add(unreachable_handler, MessageStatus.UNREACHABLE)
        hl.add(timeout_handler, MessageStatus.TIMEOUT)
        hl.add(not_ok_handler, MessageStatus.NOT_OK)
        hl.add(any_handler, MessageStatus.ANY)

        hl.call(MessageStatus.OK)

        ok_handler.assert_called_once()
        unreachable_handler.assert_not_called()
        timeout_handler.assert_not_called()
        not_ok_handler.assert_not_called()
        any_handler.assert_called_once()

    def test_handlerlist_message_unreachable(self):
        """ Test if the correct handler is called.
        """

        ok_handler = Mock()
        unreachable_handler = Mock()
        timeout_handler = Mock()
        not_ok_handler = Mock()
        any_handler = Mock()

        hl = HandlerList()
        hl.add(ok_handler, MessageStatus.OK)
        hl.add(unreachable_handler, MessageStatus.UNREACHABLE)
        hl.add(timeout_handler, MessageStatus.TIMEOUT)
        hl.add(not_ok_handler, MessageStatus.NOT_OK)
        hl.add(any_handler, MessageStatus.ANY)

        hl.call(MessageStatus.UNREACHABLE)

        ok_handler.assert_not_called()
        unreachable_handler.assert_called_once()
        timeout_handler.assert_not_called()
        not_ok_handler.assert_called_once()
        any_handler.assert_called_once()

    def test_handlerlist_message_timeout(self):
        """ Test if the correct handler is called.
        """

        ok_handler = Mock()
        unreachable_handler = Mock()
        timeout_handler = Mock()
        not_ok_handler = Mock()
        any_handler = Mock()

        hl = HandlerList()
        hl.add(ok_handler, MessageStatus.OK)
        hl.add(unreachable_handler, MessageStatus.UNREACHABLE)
        hl.add(timeout_handler, MessageStatus.TIMEOUT)
        hl.add(not_ok_handler, MessageStatus.NOT_OK)
        hl.add(any_handler, MessageStatus.ANY)

        hl.call(MessageStatus.TIMEOUT)

        ok_handler.assert_not_called()
        unreachable_handler.assert_not_called()
        timeout_handler.assert_called_once()
        not_ok_handler.assert_called_once()
        any_handler.assert_called_once()

    def test_message_ok_1(self):
        """ Test if a response message to a request results in the handler being called correctly.
        """

        conn = MockedConnection()
        chan = Channel(conn)

        conn.mock_connection_ready(True)

        req = chan.request('name', 'payload')
        handler = Mock()
        req.set_handler(handler, MessageStatus.ANY)
        reqi = req.send()

        conn.mock_downstream_verb(verbs.MessageVerb(
            messageref=1,
            status=verbs.MessageVerb.STATUS_OK,
            payload=b'response'
        ))

        self.__verify_handler_call(
            handler,
            Message,
            status=MessageStatus.OK,
            payload='response',
            source=reqi,
        )

    def test_message_unreachable_1(self):
        """ Test if a response message to a request results in the handler being called correctly.
        """

        conn = MockedConnection()
        chan = Channel(conn)

        conn.mock_connection_ready(True)

        req = chan.request('name', 'payload')
        handler = Mock()
        req.set_handler(handler, MessageStatus.ANY)
        reqi = req.send()

        conn.mock_downstream_verb(verbs.MessageVerb(
            messageref=1,
            status=verbs.MessageVerb.STATUS_UNREACHABLE,
            payload=b'response'
        ))

        self.__verify_handler_call(
            handler,
            Message,
            status=MessageStatus.UNREACHABLE,
            payload='response',
            source=reqi,
        )

    def test_message_timeout_1(self):
        """ Test if a response message to a request results in the handler being called correctly.
        """

        conn = MockedConnection()
        chan = Channel(conn)

        conn.mock_connection_ready(True)

        req = chan.request('name', 'payload')
        handler = Mock()
        req.set_handler(handler, MessageStatus.ANY)
        reqi = req.send()

        conn.mock_downstream_verb(verbs.MessageVerb(
            messageref=1,
            status=verbs.MessageVerb.STATUS_TIMEOUT,
            payload=b'response'
        ))

        self.__verify_handler_call(
            handler,
            Message,
            status=MessageStatus.TIMEOUT,
            payload='response',
            source=reqi,
        )

    def test_interest_1(self):
        """ Test if the sessions interest handler is called when an interest packet is received.
        """

        conn = MockedConnection()
        chan = Channel(conn)

        conn.mock_connection_ready(True)

        session = chan.session('name')
        handler = Mock()
        session.set_interest_handler(handler)

        conn.mock_downstream_verb(verbs.InterestVerb(
            postref=1,
            name=b'name',
            status=verbs.InterestVerb.STATUS_INTEREST,
            topic=b'topic'
        ))

        self.__verify_handler_call(
            handler,
            Interest,
            status=InterestStatus.INTEREST,
            topic='topic',
            source=session,
        )

    def test_interest_2(self):
        """ Test if the sessions interest handler is called when an interest packet is received.
        """

        conn = MockedConnection()
        chan = Channel(conn)

        conn.mock_connection_ready(True)

        session = chan.session('name')
        handler = Mock()
        session.set_interest_handler(handler)

        conn.mock_downstream_verb(verbs.InterestVerb(
            postref=1,
            name=b'name',
            status=verbs.InterestVerb.STATUS_NO_INTEREST,
            topic=b'topic'
        ))

        self.__verify_handler_call(
            handler,
            Interest,
            status=InterestStatus.NO_INTEREST,
            topic='topic',
            source=session,
        )

    def test_call_uni_1(self):
        """ Test if the sessions call handler is called when a call packet is received.
        """

        conn = MockedConnection()
        chan = Channel(conn)

        conn.mock_connection_ready(True)

        session = chan.session('name')
        handler = Mock()
        session.set_call_handler(handler)

        conn.mock_downstream_verb(verbs.CallVerb(
            unidirectional=True,
            postref=None,
            name=b'name',
            payload=b'payload',
        ))

        self.__verify_handler_call(
            handler,
            Call,
            unidirectional=True,
            postref=None,
            payload='payload',
        )

    def test_call_1(self):
        """ Test if the sessions call handler is called when an unidirectional call packet is
        received.
        """

        conn = MockedConnection()
        chan = Channel(conn)

        conn.mock_connection_ready(True)

        session = chan.session('name')
        handler = Mock()
        session.set_call_handler(handler)

        conn.mock_downstream_verb(verbs.CallVerb(
            unidirectional=False,
            postref=1,
            name=b'name',
            payload=b'payload',
        ))

        self.__verify_handler_call(
            handler,
            Call,
            unidirectional=False,
            postref=1,
            payload='payload',
        )

    def __verify_handler_call(self, handler, arg_type, **arg_attrs):
        handler.assert_called_once()
        arg = handler.call_args[0][0]

        self.assertEqual(type(arg), arg_type)

        for field, expected_value in arg_attrs.items():
            actual_value = getattr(arg, field)
            self.assertEqual(expected_value, actual_value)
