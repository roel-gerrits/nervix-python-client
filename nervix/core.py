import time
import logging
from collections import deque
from enum import Flag, auto

from nervix import verbs

logger = logging.getLogger(__name__)


class Core:

    def __init__(self, connection, serializer):

        self.connection_ready = False

        self.upstream_auto_resend = list()
        self.upstream_backlog = deque()

        self.next_messageref = 1
        self.message_handlers = dict()

        self.call_handlers = dict()
        self.interest_handlers = dict()
        self.connection_lost_handlers = set()

        self.verb_handlers = {
            verbs.MessageVerb: self.__on_message_verb,
            verbs.CallVerb: self.__on_call_verb,
            verbs.InterestVerb: self.__on_interest_verb,
            verbs.SessionVerb: self.__on_session_verb,
        }

        self.connection = connection
        self.connection.set_ready_handler(self.__on_connection_ready)
        self.connection.set_downstream_handler(self.__on_incoming_verb)

        self.serializer = serializer

    def encode_payload(self, payload):
        return self.serializer.encode(payload)

    def decode_payload(self, payload_raw):
        return self.serializer.decode(payload_raw)

    def put_upstream(self, verb, ttl=None, auto_resend=False):
        """ Called by the channel when a new verb should be send upstream.
        ttl indicates the amount of seconds a verbs should stay in the queue when it couldn't be
        send out directly. A value of None means it shouldn't stay in the queue at all, and should
        be discarded if it cannot be send immediately.
        """

        # validate the correctness of the given verb, this will raise a ValueError if something is
        # wrong, which we do not catch because it really shouldn't happen at this point.
        verb.validate()

        if auto_resend:
            self.upstream_auto_resend.append(verb)

        if self.connection_ready:
            self.connection.send_verb(verb)

        elif ttl and ttl > 0.0:
            expires = time.monotonic() + ttl
            self.upstream_backlog.appendleft((verb, expires))

    def cancel(self, verb):
        """ Cancel the given verb from being send upstream. Returns True if the verb was indeed
        successfully canceled, or False if not, possibly because the verb was already send upstream.
        """

        if verb in self.upstream_auto_resend:
            self.upstream_auto_resend.remove(verb)

        if verb in self.upstream_backlog:
            self.upstream_backlog.remove(verb)
            return True

        return False

    def new_messageref(self, handler):
        """ Register a handler and return a new unique messageref for that handler.
        """

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

            now = time.monotonic()

            for verb in reversed(self.upstream_auto_resend):
                self.upstream_backlog.appendleft((verb, None))

            while self.upstream_backlog:

                verb, expire = self.upstream_backlog.pop()

                if expire and now > expire:
                    continue

                self.connection.send_verb(verb)

        else:
            logger.info("Channel is NOT ready")

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


class Subscribe:

    def __init__(self, core, name, topic):
        self.core = core
        self.name = name
        self.topic = topic

        self.messageref = self.core.new_messageref(self.__on_message)

        self.handlers = HandlerList()

        self.verb = verbs.SubscribeVerb(
            name=encode_name(self.name),
            messageref=self.messageref,
            topic=self.core.encode_payload(self.topic)
        )

        self.core.put_upstream(self.verb, auto_resend=True)

    def set_handler(self, handler, filter=MessageStatus.ANY):
        self.handlers.add(handler, filter)

    def __on_message(self, message_verb):
        msg = Message(self.core, message_verb, self)

        filter = MessageStatus.from_verb(message_verb)
        self.handlers.call(filter, msg)

        # a messagref for a request is only used once, so we may discard it after
        # we received the response message
        self.core.discard_messageref(self.messageref)

    def cancel(self):
        res = self.core.cancel(self.verb)

        if not res:
            self.core.put_upstream(verbs.UnsubscribeVerb(
                name=encode_name(self.name),
                topic=self.core.encode_payload(self.topic))
            )

        self.core.discard_messageref(self.messageref)


class RequestStub:

    def __init__(self, core, name=None, payload=None, timeout=None, ttl=None):
        self.core = core
        self.name = name
        self.payload = payload
        self.timeout = timeout
        self.ttl = ttl

        self.default_timeout = 5.0
        self.default_ttl = 5.0

        self.handlers = HandlerList()

    def set_handler(self, handler, filter=MessageStatus.ANY):
        self.handlers.add(handler, filter)

    def send(self, name=None, payload=None, timeout=None, ttl=None):
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

    def __init__(self, core, name, payload, timeout, ttl, handlers):
        self.core = core
        self.name = name
        self.payload = payload
        self.timeout = timeout
        self.ttl = ttl
        self.handlers = handlers

        unidirectional = not bool(self.handlers)

        if unidirectional:
            self.messageref = None
        else:
            self.messageref = self.core.new_messageref(self.__on_message)

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
        self.core.cancel(self.verb)

        self.core.discard_messageref(self.messageref)


class Session:

    def __init__(self, core, name, force, persist, standby):
        self.core = core
        self.name = name
        self.force = force
        self.persist = persist
        self.standby = standby

        self.core.set_call_handler(self.name, self.__on_call)
        self.core.set_interest_handler(self.name, self.__on_interest)
        self.core.add_connection_lost_handler(self.__on_connection_lost)

        self.call_handlers = HandlerList()
        self.interest_handlers = HandlerList()

        self.current_interest = dict()

        self.verb = verbs.LoginVerb(
            name=encode_name(self.name),
            enforce=self.force,
            standby=self.standby,
            persist=self.persist,
        )

        self.core.put_upstream(self.verb, auto_resend=True)

    def set_call_handler(self, handler, filter=None):
        self.call_handlers.add(handler, filter)

    def set_interest_handler(self, handler, filter=InterestStatus.ANY):
        self.interest_handlers.add(handler, filter)

    def __on_call(self, call_verb):
        call = Call(self.core, call_verb, self)
        self.call_handlers.call(None, call)

    def __on_interest(self, interest_verb):

        # store or remove interest object in internal mapping of current interests
        topic = interest_verb.topic
        status = interest_verb.status

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
        res = self.core.cancel(self.verb)

        if not res:
            self.core.put_upstream(verbs.LogoutVerb(
                name=encode_name(self.name)
            ))

        self.core.set_call_handler(self.name, None)
        self.core.set_interest_handler(self.name, None)
        self.core.remove_connection_lost_handler(self.__on_connection_lost)


class Message:

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
        self.core = core
        self.verb = verb
        self.source = source

        self.status = InterestStatus.from_verb(verb)
        self.name = decode_name(verb.name)
        self.postref = verb.postref
        self.topic = self.core.decode_payload(verb.topic)

        self.default_ttl = 5.0

    def post(self, payload, ttl=None):
        if self.status != InterestStatus.INTEREST:
            logger.warning("Attempted post on lost interest, post will be ignored")
            return

        ttl = ttl or self.default_ttl

        post = Post(self.core, self.postref, payload, ttl)
        return post


class Post:

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
        self.core.cancel(self.verb)


class HandlerList:

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
