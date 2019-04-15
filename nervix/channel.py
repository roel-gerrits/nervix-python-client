import time
import logging
from collections import deque
from enum import Flag, auto

from nervix import verbs

from nervix.serializers.string import StringSerializer

logger = logging.getLogger(__name__)


class Channel:
    """ This is the main API interface provided for interacting with nervix servers.

    An instance of this class can be obtained by calling the create_channel() function. Which
    is the easiest way, this will also set up the mainloop for you. Alternatively one can call the
    constructor directly with a Connection instance as it's first parameter.

    This class is the starting point for all interactions with a nervix server.

    For creating subscriptions: call the subscribe() method.
    For creating sessions: call the session() method.
    for doing requests: call the request() method.
    """

    def __init__(self, connection, serializer=StringSerializer()):
        """ Constructor
        """

        self.core = Core(
            connection=connection,
            serializer=serializer,
        )

    def subscribe(self, name, topic):
        """ Subscribe to a topic on a named session.
        """

        return Subscription(
            self.core,
            name,
            topic
        )

    def session(self, name, force=False, persist=False, standby=False):
        """ Login on a session.
        """

        return Session(
            self.core,
            name,
            force,
            persist,
            standby
        )

    def request(self, name=None, payload=None, timeout=None, ttl=None):
        """ Issue a request to a named session.
        """

        return RequestStub(
            self.core,
            name,
            payload,
            timeout,
            ttl
        )


class Core:
    """ This class contains the basic logic that's used to send, re-send and receive verbs.

    The provided Connection instance is used to actually send and receive verbs.
    The given Serializer instance is used to encode and decode payloads.

    This class is used internally and should not be instantiated by the user.

    """

    def __init__(self, connection, serializer):

        # list of verbs that should be send immediately as soon as the connection
        # becomes ready
        self.upstream_auto_resend = list()

        # list of verbs that were generated when generated at a time that the connection was not ready
        # and are just put in this backlog, and will be send as soon as the connection becomes ready.
        self.upstream_backlog = deque()

        # counter used to generate unique messageref's
        self.next_messageref = 1

        # mapping of messageref's to handler functions for messages
        self.message_handlers = dict()

        # mapping of session names to handler functions for calls
        self.call_handlers = dict()

        # mapping of session names to handler functions for interests
        self.interest_handlers = dict()

        # set of handler functions to be called when the connection is lost
        self.connection_lost_handlers = set()

        # handlers for incoming verbs
        self.verb_handlers = {
            verbs.MessageVerb: self.__on_message_verb,
            verbs.CallVerb: self.__on_call_verb,
            verbs.InterestVerb: self.__on_interest_verb,
            verbs.SessionVerb: self.__on_session_verb,
        }

        # flag that indicates if the the connection is ready to send verbs to
        self.connection_ready = False

        # store the connection object and set the ready and downstream handlers
        self.connection = connection
        self.connection.set_ready_handler(self.__on_connection_ready)
        self.connection.set_downstream_handler(self.__on_incoming_verb)

        # store the serializer that is used to encode and decode payloads
        self.serializer = serializer

    def encode_payload(self, payload):
        """ Helper function to encode a payload.
        """

        return self.serializer.encode(payload)

    def decode_payload(self, payload_raw):
        """ Helper function to decode a payload.
        """

        return self.serializer.decode(payload_raw)

    def put_upstream(self, verb, ttl=None, auto_resend=False):
        """ Called when a new verb should be send upstream.
        ttl indicates the amount of seconds a verbs should stay in the queue when it couldn't be
        send out directly. A value of None means it shouldn't stay in the queue at all, and should
        be discarded if it cannot be send immediately.
        """

        # validate the correctness of the given verb, this will raise a ValueError if something is
        # wrong, which we do not catch because it really shouldn't happen at this point.
        verb.validate()

        # if the auto_resend flag is given we'll put this verb in the auto resend list
        if auto_resend:
            self.upstream_auto_resend.append(verb)

        # if the connection is ready, send the verb upstream
        if self.connection_ready:
            self.connection.send_verb(verb)

        # if not put the verb in the backlog
        elif ttl and ttl > 0.0:
            expires = time.monotonic() + ttl
            self.upstream_backlog.appendleft((verb, expires))

    def cancel(self, verb):
        """ Cancel the given verb from being send upstream. Returns True if the verb was indeed
        successfully canceled, or False if not, possibly because the verb was already send upstream.
        """

        # remove it from the auto resend list
        if verb in self.upstream_auto_resend:
            self.upstream_auto_resend.remove(verb)

        # remove it from the backlog
        if verb in self.upstream_backlog:
            self.upstream_backlog.remove(verb)
            return True

        return False

    def new_messageref(self, handler):
        """ Register a handler and return a new unique messageref for that handler.
        """

        # generate new unique messageref
        messageref = self.next_messageref
        self.next_messageref += 1

        self.message_handlers[messageref] = handler

        return messageref

    def discard_messageref(self, messageref):
        """ Discard the given messageref.
        """

        self.message_handlers.pop(messageref, None)

    def set_call_handler(self, name, handler):
        """ Set a handler for calls to the given name.
        """

        self.call_handlers[name] = handler

    def set_interest_handler(self, name, handler):
        """ Set a handler for interest to the given name.
        """

        self.interest_handlers[name] = handler

    def add_connection_lost_handler(self, handler):
        """ Add a handler that will be called when the connection is lost.
        """

        self.connection_lost_handlers.add(handler)

    def remove_connection_lost_handler(self, handler):
        """ Remove a handler from the set of connection_lost callbacks.
        """

        self.connection_lost_handlers.remove(handler)

    def __on_connection_ready(self, ready):
        """ Called by the connection to inform us if the connection is ready to send data.
        """

        self.connection_ready = ready

        if ready:
            logger.info("Channel is ready")

            # first send any verbs that are in the auto resend list
            for verb in reversed(self.upstream_auto_resend):
                self.upstream_backlog.appendleft((verb, None))

            # now send all verbs that are in the backlog and not expired yet
            now = time.monotonic()
            while self.upstream_backlog:

                verb, expire = self.upstream_backlog.pop()

                if expire and now > expire:
                    continue

                self.connection.send_verb(verb)

        else:
            logger.info("Channel is NOT ready")

            # call the connection_lost handlers
            for handler in self.connection_lost_handlers:
                handler()

    def __on_incoming_verb(self, verb):
        """ Called from the connection when a new verb is available for processing.
        """

        # validate the correctness of the given verb, this will raise a ValueError if something is
        # wrong, we will log and the verb will be ignored.
        try:
            verb.validate()
        except ValueError as exc:
            logging.warning("Received invalid %s: %s", type(verb).__name__, str(exc))
            return

        # fetch and call handler

        handler = self.verb_handlers.get(type(verb), None)

        if not handler:
            raise NotImplementedError("No handler available for this verb")

        handler(verb)

    def __on_message_verb(self, verb):
        """ Called on incoming message verbs.
        """

        messageref = verb.messageref
        handler = self.message_handlers.get(messageref, None)

        if not handler:
            logger.warning("No handler for message with messageref %s", messageref)
            return

        handler(verb)

    def __on_call_verb(self, verb):
        """ Called on incoming call verbs.
        """

        name = decode_name(verb.name)
        handler = self.call_handlers.get(name, None)

        if not handler:
            logger.warning("No handler for call to %s", name)
            return

        handler(verb)

    def __on_interest_verb(self, verb):
        """ Called on incoming interest verbs.
        """

        name = decode_name(verb.name)
        handler = self.interest_handlers.get(name, None)

        if not handler:
            logger.warning("No handler for interest to %s", name)
            return

        handler(verb)

    def __on_session_verb(self, verb):
        """ Called on incoming session verbs.
        """

        state_str = ['?', 'active', 'standby', 'ended'][verb.state]
        logger.info("Session '%s' is now %s", decode_name(verb.name), state_str)


class InterestStatus(Flag):
    NONE = 0
    INTEREST = auto()
    NO_INTEREST = auto()
    ANY = INTEREST | NO_INTEREST

    @staticmethod
    def from_verb(verb):
        if verb.status == verbs.InterestVerb.STATUS_INTEREST:
            return InterestStatus.INTEREST

        elif verb.status == verbs.InterestVerb.STATUS_NO_INTEREST:
            return InterestStatus.NO_INTEREST

        else:
            return InterestStatus.NONE


class MessageStatus(Flag):
    NONE = 0
    OK = auto()
    UNREACHABLE = auto()
    TIMEOUT = auto()
    NOT_OK = UNREACHABLE | TIMEOUT
    ANY = OK | UNREACHABLE | TIMEOUT

    @staticmethod
    def from_verb(verb):
        if verb.status == verbs.MessageVerb.STATUS_OK:
            return MessageStatus.OK

        elif verb.status == verbs.MessageVerb.STATUS_TIMEOUT:
            return MessageStatus.TIMEOUT

        elif verb.status == verbs.MessageVerb.STATUS_UNREACHABLE:
            return MessageStatus.UNREACHABLE

        else:
            return MessageStatus.NONE


class Subscription:
    """ Class used to provide an easy interface to manage subscriptions.
    Objects of this type should be obtained by calling the Channel.subscribe() method, and should
    not be instantiated directly.

    Example:

    # to subscribe
    sub = channel.subscribe('name', 'topic')
    sub.add_handler(function_to_be_called_on_incoming_messages)

    # to unsubscribe
    sub.cancel()

    """

    def __init__(self, core, name, topic):
        self.core = core
        self.name = name
        self.topic = topic

        # generate new unique messageref
        self.messageref = self.core.new_messageref(self.__on_message)

        self.handlers = HandlerList()

        # create verb and send it upstream, we set the auto_resend flag so that it will be automaticly
        # resend if the connection was lost

        self.verb = verbs.SubscribeVerb(
            name=encode_name(self.name),
            messageref=self.messageref,
            topic=self.core.encode_payload(self.topic)
        )

        self.core.put_upstream(self.verb, auto_resend=True)

    def add_handler(self, handler, filter=MessageStatus.ANY):
        """ Add a handler that should be called when a message is received for this subscription.

        filter is not really useful in this case as all messages send to a subscription have status=OK

        The handler will be called with a Message object as it's only argument.

        """
        self.handlers.add(handler, filter)

    def __on_message(self, message_verb):
        """ Called when a message is received for this subscription.
        """

        # create Message object
        msg = Message(self.core, message_verb, self)

        # send Message object to all handlers that match the filter
        filter = MessageStatus.from_verb(message_verb)
        self.handlers.call(filter, msg)

    def cancel(self):
        """ Cancel the subscription.
        """

        # try to cancel the subscribe verb
        res = self.core.cancel(self.verb)

        # if canceling was not successful it means that it was already send to the server
        # and we thus have to send an unsubscribe to really cancel it.
        if not res:
            self.core.put_upstream(verbs.UnsubscribeVerb(
                name=encode_name(self.name),
                topic=self.core.encode_payload(self.topic))
            )

        # get rid of the messageref as we don't need it anymore
        self.core.discard_messageref(self.messageref)


class RequestStub:
    """ Class used to provide an easy interface to send requests.

    Objects of this type should be obtained by calling the Channel.request() method, and should
    not be instantiated directly.

    Example:

    # to send an unidirectional request (meaning we DO NOT expect a response)
    req = channel.request('name', 'payload')
    req.send()

    # to send an bidirectional request (meaning we DO expect a response)
    req = channel.request('name', 'payload')
    req.add_handler(function_to_be_called_when_response_arrives)
    req.send()

    # to specify different handlers for different message statuses
    req = channel.request('name', 'payload')
    req.add_handler(function_to_be_called_on_success, MessageStatus.OK)
    req.add_handler(function_to_be_called_on_errors, MessageStatus.NOT_OK)
    req.add_handler(function_to_be_called_on_timeouts, MessageStatus.TIMEOUT)
    req.add_handler(function_to_be_called_on_unreachable, MessageStatus.UNREACHABLE)
    req.send()

    # to send the same message multiple times
    req = channel.request('name', 'payload')
    req.send()
    req.send()
    req.send()

    # to send the same message multiple times but with different payloads
    req = channel.request('name')
    req.send(payload='payload1')
    req.send(payload='payload2')
    req.send(payload='payload3')

    """

    def __init__(self, core, name=None, payload=None, timeout=None, ttl=None):
        self.core = core
        self.name = name
        self.payload = payload
        self.timeout = timeout
        self.ttl = ttl

        self.default_timeout = 5.0
        self.default_ttl = 5.0

        self.handlers = HandlerList()

    def add_handler(self, handler, filter=MessageStatus.ANY):
        """ Add a handler that should be called when a message is received for this subscription.

        filter can be used to add handlers that will only be called if the message has a certain status.

        The handler will be called with a Message object as it's only argument.

        """
        self.handlers.add(handler, filter)

    def send(self, name=None, payload=None, timeout=None, ttl=None):
        """ Send the request.
        It is possibble to override any of the arguments initially given to the classes constructor.
        This allows for the creation of a single RequestStub object that can be used to send multiple
        similar requests.

        """
        # use stub attribute if no local attribute is given
        name = name or self.name
        payload = payload or self.payload
        timeout = timeout or self.timeout
        ttl = ttl if ttl is not None else self.ttl

        # use default values if value was not yet set
        timeout = timeout or self.default_timeout
        ttl = ttl if ttl is not None else self.default_ttl

        # create request object
        request = Request(self.core, name, payload, timeout, ttl, self.handlers)
        return request


class Request:
    """ Class used to represent a single request.

    This class should not be instantiated directly but should only be obtained by calling
    the RequestStub.send() method.
    """

    def __init__(self, core, name, payload, timeout, ttl, handlers):
        self.core = core
        self.name = name
        self.payload = payload
        self.timeout = timeout
        self.ttl = ttl
        self.handlers = handlers

        unidirectional = not bool(self.handlers)

        # fetch messageref, but only if the request is bidirectional
        if unidirectional:
            self.messageref = None
        else:
            self.messageref = self.core.new_messageref(self.__on_message)

        # create the verb and send it upstream
        self.verb = verbs.RequestVerb(
            name=encode_name(self.name),
            unidirectional=unidirectional,
            messageref=self.messageref,
            timeout=self.timeout,
            payload=self.core.encode_payload(self.payload),
        )

        self.core.put_upstream(self.verb, ttl=self.ttl)

    def __on_message(self, message_verb):
        msg = Message(self.core, message_verb, self)

        filter = MessageStatus.from_verb(message_verb)
        self.handlers.call(filter, msg)

        # a messagref for a request is only used once, so we may discard it after
        # we received the response message
        self.core.discard_messageref(self.messageref)

    def cancel(self):
        """ Cancel the request.
        This will only have effect if the request was not send yet because the connection was down, and
        is still in the backlog queue.
        """

        res = self.core.cancel(self.verb)

        if not res:
            logger.info('Request cancelation had no effect as it was already send.')

        self.core.discard_messageref(self.messageref)


class Session:
    """ Class used to provide an easy interface for managing sessions.

    Objects of this type should be obtained by calling the Channel.session() method, and should
    not be instantiated directly.

    Example:

    # to claim a session with the name 'demo'
    sess = channel.session('demo')

    # to add a handler for incoming calls
    sess.add_call_handler(function_to_be_called_on_calls)

    # to add a handler for incoming interests
    sess.add_interest_handler(function_to_be_called_on_interest)

    # to cancel the session
    sess.cancel()

    """

    def __init__(self, core, name, force, persist, standby):
        self.core = core
        self.name = name
        self.force = force
        self.persist = persist
        self.standby = standby

        # set handlers for events from core
        self.core.set_call_handler(self.name, self.__on_call)
        self.core.set_interest_handler(self.name, self.__on_interest)
        self.core.add_connection_lost_handler(self.__on_connection_lost)

        self.call_handlers = HandlerList()
        self.interest_handlers = HandlerList()

        # dict used to keep track of what interests present
        self.current_interest = dict()

        # send login verb
        self.verb = verbs.LoginVerb(
            name=encode_name(self.name),
            enforce=self.force,
            standby=self.standby,
            persist=self.persist,
        )

        self.core.put_upstream(self.verb, auto_resend=True)

    def add_call_handler(self, handler, filter=None):
        """ Add a handler that should be called on incoming calls.

        The filter argument has currently no use.

        The handler will be called with a Call object as its only argument.
        """

        self.call_handlers.add(handler, filter)

    def add_interest_handler(self, handler, filter=InterestStatus.ANY):
        """ Add a handler that will be called on interest updates.

        The filter agument can be used to specify different handlers for
        INTEREST or NO_INTEREST.

        The handler will be called with a Interest object as its only argument.

        """
        self.interest_handlers.add(handler, filter)

    def __on_call(self, call_verb):
        call = Call(self.core, call_verb, self)
        self.call_handlers.call(None, call)

    def __on_interest(self, interest_verb):

        # store or remove interest object in internal mapping of current interests
        topic = interest_verb.topic
        status = interest_verb.status

        # store interest in local interest_dict, this is needed to simulate NO_INTEREST
        # verbs when the connection is lost (see __on_connection_lost method)
        if status == verbs.InterestVerb.STATUS_INTEREST:
            self.current_interest[topic] = interest_verb
        elif status == verbs.InterestVerb.STATUS_NO_INTEREST:
            self.current_interest.pop(topic, None)

        interest = Interest(self.core, interest_verb, self)
        self.interest_handlers.call(interest.status, interest)

    def __on_connection_lost(self):
        """ Called when the connection is lost.
        """

        # simulate a NO_INTEREST verb for all currently known interests
        while self.current_interest:
            topic, verb = self.current_interest.popitem()
            verb.status = verbs.InterestVerb.STATUS_NO_INTEREST

            self.__on_interest(verb)

    def cancel(self):
        """ Cancel the session.
        """

        res = self.core.cancel(self.verb)

        if not res:
            self.core.put_upstream(verbs.LogoutVerb(
                name=encode_name(self.name)
            ))

        self.core.set_call_handler(self.name, None)
        self.core.set_interest_handler(self.name, None)
        self.core.remove_connection_lost_handler(self.__on_connection_lost)


class Message:
    """ Class used to represent an incoming message.

    An instance of this class is passed to the handlers on requests and subscriptions.
    e.g. those set with Request.add_handler() and Subscription.add_handler()

    """

    def __init__(self, core, verb, source):
        self.core = core
        self.verb = verb
        self.source = source

        self.status = MessageStatus.from_verb(verb)

        if self.status == MessageStatus.OK:
            self.payload = self.core.decode_payload(verb.payload)

        else:
            self.payload = None

    def __repr__(self):
        return f"Message({self.status.name}, {repr(self.payload)})"


class Call:
    """ Class used to represent an incoming call.

    An instance of this class is passed to the handlers for calls, e.g. those set
    with Session.add_call_handler()

    The post() method can be used to send a response to a call.

    Example:

    # to post an answer to a call
    call.post('the answer')

    # to specify a ttl other then the default ttl:
    call.post('the answer', ttl=10.0)

    """

    def __init__(self, core, verb, source):
        self.core = core
        self.verb = verb
        self.source = source

        self.unidirectional = verb.unidirectional
        self.name = decode_name(verb.name)
        self.postref = verb.postref
        self.payload = self.core.decode_payload(verb.payload)

        self.default_ttl = 5.0

    def post(self, payload, ttl=None):
        """ Post a response to the call.

        If the call was unidirectional a warning will be logged and no reply will actually
        be send.
        """

        if self.unidirectional:
            logger.warning("Post done on unidirectional call, it will be ignored")
            return

        ttl = ttl or self.default_ttl

        post = Post(self.core, self.postref, payload, ttl)
        return post

    def __repr__(self):
        return f"Call({self.name}, {repr(self.payload)})"


class Interest:

    def __init__(self, core, verb, source):
        """ Class used to represent an incoming interest.

        An instance of this class is passed to the handlers for interests, e.g. those set
        with Session.add_interest_handler()

        The post() method can be used to send a response to an interest.

        Example:

        # to post an answer to an interest
        interest.post('the answer')

        # to specify a ttl other then the default ttl:
        interest.post('the answer', ttl=10.0)

        """

        self.core = core
        self.verb = verb
        self.source = source

        self.status = InterestStatus.from_verb(verb)
        self.name = decode_name(verb.name)
        self.postref = verb.postref
        self.topic = self.core.decode_payload(verb.topic)

        self.default_ttl = 5.0

    def post(self, payload, ttl=None):
        """ Post a response to an interest.

        If the there was no longer interest a warning will be logged and no post will actually
        be send.
        """

        if self.status != InterestStatus.INTEREST:
            logger.warning("Attempted post on lost interest, post will be ignored")
            return

        ttl = ttl or self.default_ttl

        post = Post(self.core, self.postref, payload, ttl)
        return post


class Post:
    """ Class used to represent a post.

    An instance of this class is returned by the Call.post() and Interest.post() methods,
    and can be used to cancel a post if desired.

    """

    def __init__(self, core, postref, payload, ttl):
        self.core = core
        self.postref = postref
        self.payload = payload
        self.ttl = ttl

        self.verb = verbs.PostVerb(
            postref=self.postref,
            payload=self.core.encode_payload(self.payload),
        )

        self.core.put_upstream(self.verb, ttl=self.ttl)

    def cancel(self):
        res = self.core.cancel(self.verb)

        if not res:
            logger.info('Post cancelation had no effect as it was already send.')


class HandlerList:
    """ Class used internally to easely manage multiple handlers and allow filters to be used.
    """

    def __init__(self):
        self.handlers = list()

    def add(self, handler, filter):
        self.handlers.append((handler, filter))

    def call(self, call_filter, *args, **kwargs):

        for handler, handler_filter in self.handlers:

            if call_filter is None or call_filter & handler_filter:
                handler(*args, **kwargs)

    def __len__(self):
        return len(self.handlers)


def encode_name(name):
    """ Encode and validate the name. This converts the given string to a bytes object, beause
    that is what we need to actually send it over the wire later. Also verify that the name is
    a valid name.
    """

    name_b = name.encode()

    if len(name_b) > 255:
        raise ValueError("Name exceeded maximum length of 255 characters")

    if len(name_b) < 1:
        raise ValueError("Name is shorter than minimium length of 1 character")

    for c in name_b:
        # test for 0-9, A-Z, a-z, -, _
        if not (48 <= c <= 57 or 65 <= c <= 90 or 97 <= c <= 122 or c == ord('-') or c == ord('_')):
            raise ValueError("Name contains invalid character '%s' (%d)" % (chr(c), c))

    return name_b


def decode_name(bts):
    """ Decode a encoded name back to utf-8
    """

    return bts.decode()
