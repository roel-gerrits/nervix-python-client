from nervix.core import Core, Subscribe, Session, RequestStub
from nervix.serializers.string import StringSerializer


class Channel:

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

        return Subscribe(
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
