from collections import deque


class EventQueue:

    def __init__(self):
        self.queue = deque()

    def append(self, event):
        self.queue.append(event)

    def verify(self, verify_kind, **verify_attrs):
        """ Verify if the given event kind and attrs is valid according to the current state
        of the queue.
        """

        if not self.queue:
            raise ValueError("There are no events in the eventqueue")

        expected = self.queue[0]
        expected_kind = type(expected)

        if expected_kind != verify_kind:
            raise ValueError(f"The expected kind {expected_kind.__name__} does not match "
                             f"the verified kind {verify_kind.__name__}")

        result = expected.verify(**verify_attrs)

        # if the result is True the event is fully verified and can be discarded from the queue
        # if not, then the event is not fully verified yet, and it should stay on the queue a
        # little longer
        if result:
            self.queue.popleft()

    def peek(self, kind):
        """ Check if the current pending event matches the given event, and if so return it.
        """

        if not self.queue:
            return None

        event = self.queue[0]

        if type(event) != kind:
            return None

        return event

    def get_events(self, filter_set):
        """ Return the events that are currently in the front end of the queue and match the filter set
        """

        for event in self.queue:

            if type(event) not in filter_set:
                break

            yield event

    def __len__(self):
        return len(self.queue)

    def dump(self):
        print('eventqueue = [')
        for event in self.queue:
            print('        ', event, '. ')
        print('    ]')


class ExpectEvent:

    def __init__(self, **attrs):
        self.__dict__ = attrs

    def verify(self, **verify_attrs):

        kind = self.__class__.__name__

        for expected_attr, expected_value in self.__dict__.items():

            if expected_attr not in verify_attrs:
                raise ValueError(f"Missing attr {expected_attr} while validating {kind} event")

            verify_value = verify_attrs[expected_attr]

            if expected_value != verify_value:
                raise ValueError(f"Value missmatch, expected '{expected_value}' instead of '{verify_value}' "
                                 f"while validating {kind}")

        return True


class ExpectSleep(ExpectEvent):

    def __init__(self, duration):
        self.duration = duration

    def verify(self, duration=None):
        self.duration -= duration

        if self.duration < 0:
            raise ValueError(f"Slept for too long ({-self.duration:.1f}s too long)")

        elif self.duration == 0:
            # done sleeping
            return True

        else:
            # not yet done sleeping
            return False


class ExpectTcpSyn(ExpectEvent):
    pass


class ExpectTcpSynAck(ExpectEvent):
    pass


class ExpectTcpOutput(ExpectEvent):
    pass


class ExpectTcpRst(ExpectEvent):
    pass


class DoEvent:

    def __init__(self, **attrs):
        self.__dict__ = attrs
        # self.expected_attrs = attrs

    def verify(self, **verify_attrs):

        kind = self.__class__.__name__

        for expected_attr, expected_value in self.__dict__.items():

            if expected_attr not in verify_attrs:
                raise ValueError(f"Missing attr {expected_attr} while validating {kind} event")

            verify_value = verify_attrs[expected_attr]

            if expected_value != verify_value:
                raise ValueError(f"Value missmatch, expected '{expected_value}' instead of '{verify_value}' "
                                 f"while validating {kind}")

        return True


class DoTcpSyn(DoEvent):
    pass


class DoTcpSynAck(DoEvent):
    pass


class DoTcpInput(DoEvent):
    pass


class DoTcpRst(DoEvent):
    pass
