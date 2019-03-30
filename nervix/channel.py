import logging

from enum import Flag, auto

from nervix import verbs
from nervix.core import Core

logger = logging.getLogger(__name__)


class Channel:

    def __init__(self, connection):
        self.core = Core(connection)

    def subscribe(self, name, topic):
        """ Subscribe to a topic on a named session.
        """

        return Subscribe(self.core, name, topic)

    def session(self, name, force=False, persist=False, standby=False):
        """ Login on a session.
        """

        return Session(self.core, name, force, persist, standby)

    def request(self, name=None, payload=None, timeout=None, ttl=None):
        """ Issue a request to a named session.
        """

        return RequestStub(self.core, name, payload, timeout, ttl)


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
            name=self.name,
            messageref=self.messageref,
            topic=self.topic
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
                name=self.name,
                topic=self.topic)
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
            name=self.name,
            unidirectional=unidirectional,
            messageref=self.messageref,
            timeout=self.timeout,
            payload=self.payload
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

        self.call_handlers = HandlerList()
        self.interest_handlers = HandlerList()

        self.verb = verbs.LoginVerb(
            name=self.name,
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
        interest = Interest(self.core, interest_verb, self)
        filter = InterestStatus.from_verb(interest_verb)
        self.interest_handlers.call(filter, interest)

    def cancel(self):
        res = self.core.cancel(self.verb)

        if not res:
            self.core.put_upstream(verbs.LogoutVerb(
                name=self.name
            ))

        self.core.set_call_handler(self.name, None)
        self.core.set_interest_handler(self.name, None)


class Message:

    def __init__(self, core, verb, source):
        self.core = core
        self.verb = verb
        self.source = source

        self.status = MessageStatus.from_verb(verb)
        self.payload = verb.payload


class Call:

    def __init__(self, core, verb, source):
        self.core = core
        self.verb = verb
        self.source = source

        self.unidirectional = verb.unidirectional
        self.postref = verb.postref
        self.payload = verb.payload

        self.default_ttl = 5.0

    def post(self, payload, ttl=None):
        if self.unidirectional:
            logging.warning("Post done on unidirectional call, it will be ignored")
            return

        ttl = ttl or self.default_ttl

        post = Post(self.core, self.postref, payload, ttl)
        return post


class Interest:

    def __init__(self, core, verb, source):
        self.core = core
        self.verb = verb
        self.source = source

        self.status = InterestStatus.from_verb(verb)
        self.postref = verb.postref
        self.topic = verb.topic

        self.default_ttl = 5.0

    def post(self, payload, ttl=None):
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
            payload=self.payload,
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
