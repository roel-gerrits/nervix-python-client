import time
import logging
from collections import deque

from nervix import verbs

logger = logging.getLogger(__name__)


class Core:

    def __init__(self, connection):
        self.connection = connection

        self.connection.set_ready_handler(self.__on_connection_ready)
        self.connection.set_downstream_handler(self.__on_incoming_verb)

        self.connection_ready = False

        self.upstream_auto_resend = list()
        self.upstream_backlog = deque()

        self.next_messageref = 1
        self.message_handlers = dict()

        self.call_handlers = dict()
        self.interest_handlers = dict()

        self.verb_handlers = {
            verbs.MessageVerb: self.__on_message_verb,
            verbs.CallVerb: self.__on_call_verb,
            verbs.InterestVerb: self.__on_interest_verb,
            verbs.SessionVerb: self.__on_session_verb,
        }

    def put_upstream(self, verb, ttl=None, auto_resend=False):
        """ Called by the channel when a new verb should be send upstream.
        ttl indicates the amount of seconds a verbs should stay in the queue when it couldn't be
        send out directly. A value of None means it shouldn't stay in the queue at all, and should
        be discarded if it cannot be send immediately.
        """

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

        # todo

    def set_call_handler(self, name, handler):
        """ Set a handler for calls to the given name.
        """

        self.call_handlers[name] = handler

    def set_interest_handler(self, name, handler):
        """ Set a handler for interest to the given name.
        """

        self.interest_handlers[name] = handler

    def __on_connection_ready(self, ready):
        """ Called by the connection to inform us if the connection is ready to send data.
        """

        self.connection_ready = ready

        if ready:
            now = time.monotonic()

            for verb in reversed(self.upstream_auto_resend):
                self.upstream_backlog.appendleft((verb, None))

            while self.upstream_backlog:

                verb, expire = self.upstream_backlog.pop()

                if expire and now > expire:
                    continue

                self.connection.send_verb(verb)

    def __on_incoming_verb(self, verb):
        """ Called from the connection when a new verb is available for processing.
        """

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

        name = verb.name
        handler = self.call_handlers.get(name, None)

        if not handler:
            logger.warning("No handler for call to %s", name)
            return

        handler(verb)

    def __on_interest_verb(self, verb):
        """ Called on incoming interest verbs.
        """

        name = verb.name
        handler = self.interest_handlers.get(name, None)

        if not handler:
            logger.warning("No handler for interest to %s", name)
            return

        handler(verb)

    def __on_session_verb(self, verb):
        """ Called on incoming session verbs.
        """
