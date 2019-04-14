from collections import deque


class Event:

    def __init__(self, **kwargs):

        for field, value in kwargs.items():
            setattr(self, field, value)

        self.attributes = kwargs.keys()

        self.__is_done = False

    def done(self):
        self.__is_done = True

    def is_done(self):
        return self.__is_done

    def match(self, other):
        if type(other) != self.__class__:
            return False

        for attr in other.attributes:

            other_value = getattr(other, attr)
            this_value = getattr(self, attr)
            if this_value != other_value:
                return False

        return True

    #
    # def __getattr__(self, attr):
    #     if attr in self.attributes:
    #         return self.attributes[attr]
    #
    #     raise AttributeError(f"{self.__class__.__name__} has no attribute '{attr}'")
    #
    # def __setattr__(self, key, value):
    #     if 'attributes' not in self.__dict__:
    #         super().__setattr__(key, value)
    #
    #     elif key in self.attributes:
    #         self.attributes[key] = value
    #
    #     else:
    #         self.__dict__[key] = value

    def __repr__(self):
        args = [(field, value) for (field, value) in self.__dict__.items() if field in self.attributes]

        args_str = ' '.join([f'{field}={value}' for field, value in self.__dict__.items() if field in self.attributes])
        return f"{self.__class__.__name__}({args_str})"


class Sleep(Event):

    def subtract_time(self, duration):
        self.duration -= duration

        if self.duration == 0.0:
            self.done()

        if self.duration < 0.0:
            raise VerifyError(f"Slept {self.duration} too long")

        return self.duration


class TcpEvent(Event):
    pass


class OutgoingTcpSyn(TcpEvent):
    pass


class OutgoingTcpSynAck(TcpEvent):
    pass


class OutgoingTcpData(TcpEvent):
    pass


class OutgoingTcpFin(TcpEvent):
    pass


class IncomingTcpSyn(TcpEvent):
    pass


class IncomingTcpSynAck(TcpEvent):
    pass


class IncomingTcpData(TcpEvent):

    def get_chunk(self, limit):
        chunk = self.data[:limit]
        self.data = self.data[limit:]

        if len(self.data) == 0:
            self.done()

        return chunk


class IncomingTcpFin(TcpEvent):
    pass


class EventQueue:

    def __init__(self):
        self.events = deque()

    def fetch(self, actual_event):

        planned_event = self.head()

        if not planned_event.match(actual_event):
            return None

        return planned_event

    def expect(self, actual_event):

        planned_event = self.head()

        if not planned_event:
            raise VerifyError("No events planned.")

        if not planned_event.match(actual_event):
            print('?')
            print('? ACTUAL:   ', actual_event)
            print("?             != ")
            print('? PLANNED:  ', planned_event)
            print('?')

            raise VerifyError("Event verification error")

        # planned_event.done()

        return planned_event

    def add(self, event):
        self.events.append(event)

    def scan(self, filter_set):
        """ Return the events that are currently in the front
        of the queue and match the filter set
        """

        for event in self.events:

            if type(event) not in filter_set:
                break

            yield event

    def head(self):
        while self.events:
            event = self.events[0]
            if event.is_done():
                self.events.popleft()

            else:
                return event

        return None

    def __len__(self):
        self.head()
        return len(self.events)

    def dump(self):
        print('eventqueue = [')
        for event in self.events:
            print('        ', event, '. ')
        print('    ]')


class VerifyError(AssertionError):
    pass
