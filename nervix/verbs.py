class BaseVerb:

    def validate(self):
        raise NotImplementedError("The validate() method is not implemented for this verb")

    def __repr__(self):
        return "<{cls}>".format(
            cls=self.__class__.__name__
        )

    def __eq__(self, other):
        return self.__dict__ == other.__dict__


class LoginVerb(BaseVerb):

    def __init__(self, name=None, enforce=None, standby=None, persist=None):
        self.name = name
        self.enforce = enforce
        self.standby = standby
        self.persist = persist

    def validate(self):
        validate_name(self.name)
        validate_bool(self.enforce)
        validate_bool(self.standby)
        validate_bool(self.persist)


class LogoutVerb(BaseVerb):

    def __init__(self, name=None):
        self.name = name

    def validate(self):
        validate_name(self.name)


class SessionVerb(BaseVerb):
    STATE_ACTIVE = 1
    STATE_STANDBY = 2
    STATE_ENDED = 3

    def __init__(self, name=None, state=None):
        self.name = name
        self.state = state

    def validate(self):
        validate_name(self.name)
        validate_enum(self.state, [self.STATE_ACTIVE, self.STATE_STANDBY, self.STATE_ENDED])

    def __repr__(self):
        return "<{cls} {name} {state}>".format(
            cls=self.__class__.__name__,
            name=self.name.decode() if isinstance(self.name, bytes) else self.name,
            state=[None, 'ACTIVE', 'STANDBY', 'ENDED'][self.state],
        )


class RequestVerb(BaseVerb):

    def __init__(self, name=None, unidirectional=None, messageref=None, timeout=None, payload=None):
        self.name = name
        self.unidirectional = unidirectional
        self.messageref = messageref
        self.timeout = timeout
        self.payload = payload

    def validate(self):
        validate_name(self.name)
        validate_bool(self.unidirectional)
        validate_refnr(self.messageref)
        validate_timeout(self.timeout)
        validate_payload(self.payload)

    def __repr__(self):
        return "{cls}({name}, unidirectional={unidirectional}, message_ref={messageref}, timeout={timeout}, '{payload}')".format(
            cls=self.__class__.__name__,
            **self.__dict__
        )


class CallVerb(BaseVerb):

    def __init__(self, unidirectional=None, postref=None, name=None, payload=None):
        self.unidirectional = unidirectional
        self.postref = postref
        self.name = name
        self.payload = payload

    def validate(self):
        validate_bool(self.unidirectional)
        validate_refnr(self.postref)
        validate_name(self.name)
        validate_payload(self.payload)

    def __repr__(self):
        return "{cls}({postref}, unidirectional={unidirectional}, '{payload}')".format(
            cls=self.__class__.__name__,
            **self.__dict__
        )


class PostVerb(BaseVerb):

    def __init__(self, postref=None, payload=None):
        self.postref = postref
        self.payload = payload

    def validate(self):
        validate_refnr(self.postref)
        validate_payload(self.payload)

    def __repr__(self):
        return "{cls}({postref}, '{payload}')".format(
            cls=self.__class__.__name__,
            **self.__dict__
        )


class MessageVerb(BaseVerb):
    STATUS_OK = 0
    STATUS_TIMEOUT = 1
    STATUS_UNREACHABLE = 2

    def __init__(self, messageref=None, status=None, payload=None):
        self.messageref = messageref
        self.status = status
        self.payload = payload

    def validate(self):
        validate_refnr(self.messageref)
        validate_enum(self.status, [self.STATUS_OK, self.STATUS_TIMEOUT, self.STATUS_UNREACHABLE])
        validate_payload(self.payload)

    def __repr__(self):
        return "{cls}({messageref}, status={statusstr}, '{payload}')".format(
            cls=self.__class__.__name__,
            statusstr=['OK', 'TIMEOUT', 'UNREACHABLE'][self.status],
            **self.__dict__
        )


class SubscribeVerb(BaseVerb):

    def __init__(self, name=None, messageref=None, topic=None):
        self.name = name
        self.messageref = messageref
        self.topic = topic

    def validate(self):
        validate_name(self.name)
        validate_refnr(self.messageref)
        validate_payload(self.topic)

    def __repr__(self):
        return "{cls}({name}, message_ref={messageref}, '{topic}')".format(
            cls=self.__class__.__name__,
            **self.__dict__
        )


class InterestVerb(BaseVerb):
    STATUS_NO_INTEREST = 0
    STATUS_INTEREST = 1

    def __init__(self, postref=None, name=None, status=None, topic=None):
        self.postref = postref
        self.name = name
        self.status = status
        self.topic = topic

    def validate(self):
        validate_refnr(self.postref)
        validate_name(self.name)
        validate_enum(self.status, [self.STATUS_NO_INTEREST, self.STATUS_INTEREST])
        validate_payload(self.topic)

    def __repr__(self):
        return "{cls}({postref}, {status_str}, '{topic}')".format(
            cls=self.__class__.__name__,
            status_str=['NO_INTEREST', 'INTEREST'][self.status],
            **self.__dict__
        )


class UnsubscribeVerb(BaseVerb):

    def __init__(self, name=None, topic=None):
        self.name = name
        self.topic = topic

    def validate(self):
        validate_name(self.name)
        validate_payload(self.topic)

    def __repr__(self):
        return "{cls}({name}, '{topic}')".format(
            cls=self.__class__.__name__,
            **self.__dict__
        )


def validate_name(name):
    if type(name) != bytes:
        raise ValueError("Name is not of 'bytes' type")

    if len(name) > 255:
        raise ValueError("Name exceeded maximum length of 255 characters")

    if len(name) < 1:
        raise ValueError("Name is shorter than minimium length of 1 character")

    for c in name:
        # test for 0-9, A-Z, a-z, -, _
        if not (48 <= c <= 57 or 65 <= c <= 90 or 97 <= c <= 122 or c == ord('-') or c == ord('_')):
            raise ValueError("Name contains invalid character '%s' (%d)" % (chr(c), c))


def validate_bool(value):
    if type(value) != bool:
        raise ValueError("Expected value to be of type bool not '%s'" % type(value))


def validate_refnr(nr):
    if nr is None:
        return

    elif nr <= 0:
        raise ValueError("Reference number must be greater then zero")

    elif nr > 2 ** 32:
        raise ValueError("Reference number must be less then 2^32")


def validate_timeout(timeout):
    if timeout is None:
        return

    elif timeout < 0:
        raise ValueError("Timeout must be greater or equal to zero")


def validate_payload(payload):
    if payload is None:
        return

    if type(payload) != bytes:
        raise ValueError("Payload is of type '%s' not bytes" % type(payload))

    if len(payload) > 2 ** 15:
        raise ValueError("Payload size should not exceed 32Kb")


def validate_enum(value, allowed_values):
    if value is None:
        return

    if value not in allowed_values:
        raise ValueError("Given enum value (%s) is not in the list of allowed values %s" % (value, allowed_values))


def validate_align_mark(mark):
    if mark is None:
        return

    elif mark <= 0:
        raise ValueError("Mark must be greater then zero")

    elif mark > 2 ** 32:
        raise ValueError("Mark must be less then 2^32")
