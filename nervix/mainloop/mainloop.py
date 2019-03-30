import time
import heapq

try:
    import selectors
except ImportError:
    # allows us to use pypy3
    import compat.selectors as selectors

import logging

from .control import Control

logger = logging.getLogger(__name__)


class Mainloop:
    """
    Mainloop class that handles IO and timer events.

    By registering a filedescriptor using the register() method, a
    IOProxy object is achieved. This object is used to let the mainloop
    know in which events we're interested and what functions should be
    called on a certain event.

    By creating a timer using the timer() method, a new timer is
    achieved. This object is used to let the mainloop know what function
    to call when the timer expires, and to set the timer timeout.

    All this is executed by running the mainloop. This should be done by
    calling the run_once() or run_forever() methods.

    The run_once method will run the loop for only a few events or when
    the timeout expires, after which it returns.

    The run_forever method will run the loop forever until the
    shutdown() method is called. The mainloop will then run for at most
    one more cycle.
    """

    SIG_WAKEUP = 0
    SIG_SHUTDOWN = 1

    def __init__(self):

        self.selector = selectors.DefaultSelector()

        self.fd_events = dict()
        self.fd_read_handlers = dict()
        self.fd_write_handlers = dict()

        self.timer_deadlines = list()
        self.timer_handlers = dict()

        self.control = Control(self)

        self.shutdown_flag = False

    def now(self):
        """ Return the current monotonic timestamp
        """

        return time.monotonic()

    def shutdown(self):
        self.control.signal(Mainloop.SIG_SHUTDOWN)

    def run_forever(self):

        while not self.shutdown_flag:
            self.run_once(None)

    def run_once(self, max_timeout=None):
        """
        Run for one cycle.
        """

        # stats
        nr_timers = 0
        nr_writes = 0
        nr_reads = 0
        nr_signals = 0

        # retrieve the remaining time for the first timer to expire
        timer_timeout = self._get_next_timer_deadline()

        # calculate the timeout used for the select() call
        timeout = None

        if max_timeout and timer_timeout:
            timeout = min(max_timeout, timer_timeout)

        elif max_timeout:
            timeout = max_timeout

        elif timer_timeout:
            timeout = timer_timeout

        # wait for events
        events = self.selector.select(timeout)

        # process expired timers
        expired_timers = self._get_expired_timers()

        for key in expired_timers:

            nr_timers += 1

            handler = self.timer_handlers.pop(key, None)

            # if handler is None, it means the timer is canceled.

            if handler:
                handler()

        # process IO events
        for key, mask in events:

            if mask & selectors.EVENT_WRITE:

                nr_writes += 1

                handler = self.fd_write_handlers.get(key.fd, None)
                if handler:
                    handler()

            if mask & selectors.EVENT_READ:

                nr_reads += 1

                handler = self.fd_read_handlers.get(key.fd, None)
                if handler:
                    handler()

        # process control signals
        for signal in self.control.signals():

            nr_signals += 1

            if signal == Mainloop.SIG_SHUTDOWN:
                self.shutdown_flag = True

        return nr_timers + nr_writes + nr_reads + nr_signals

    def register(self, fd):
        """
        Register a filedescriptor on the mainloop, returning an IOProxy
        object.
        """

        proxy = IOProxy(self, fd)
        return proxy

    def timer(self):
        """
        Create a new timer on the mainloop, returning a Timer object.
        """

        timer = Timer(self)
        return timer

    def _update_interest(self, fd, read=None, write=None):
        """
        Modify the events that the selector should select.
        """

        new_events = self.fd_events.get(fd, 0)
        old_events = new_events

        # calculate bitmask
        if read is True:
            new_events |= selectors.EVENT_READ

        elif read is False:
            new_events &= ~selectors.EVENT_READ

        if write is True:
            new_events |= selectors.EVENT_WRITE

        elif write is False:
            new_events &= ~selectors.EVENT_WRITE

        # register, modify or unregister
        if new_events and not old_events:
            self.selector.register(fd, new_events)

        elif new_events:
            self.selector.modify(fd, new_events)

        else:
            self.selector.unregister(fd)

        self.fd_events[fd] = new_events

    def _update_read_handler(self, fd, func):
        """
        Set the handler for that will be called on read events.
        """

        self.fd_read_handlers[fd] = func

    def _update_write_handler(self, fd, func):
        """
        Set the handler that will be called on write events.
        """

        self.fd_write_handlers[fd] = func

    def _unregister(self, fd):
        if fd in self.fd_events:
            self._update_interest(fd, read=False, write=False)
            del self.fd_events[fd]
            self.fd_read_handlers.pop(fd, None)
            self.fd_write_handlers.pop(fd, None)

    def _update_timer_timeout(self, timer_id, timeout):
        """
        Set the timeout after which the timer will expire
        """

        now = self.now()
        deadline = now + timeout

        heapq.heappush(self.timer_deadlines, (deadline, timer_id))

        self.control.signal(Mainloop.SIG_WAKEUP)

    def _update_timer_handler(self, timer_id, func):
        """
        Set the handler that will be called when a timer expires.
        """

        if callable(func):
            self.timer_handlers[timer_id] = func

        else:
            if timer_id in self.timer_handlers:
                del self.timer_handlers[timer_id]

    def _get_next_timer_deadline(self):
        """
        Calculate the time that's left until the next timer will expire.
        Returns None if there are no timers to expire.
        """

        timeout = None

        while self.timer_deadlines and not timeout:

            now = self.now()

            deadline, timer_id = self.timer_deadlines[0]

            # skip timers that have no handler attached
            if timer_id not in self.timer_handlers:
                heapq.heappop(self.timer_deadlines)
                continue

            timeout = deadline - now

        return timeout

    def _get_expired_timers(self):
        """
        Returns a list of the timers that have expired
        """

        now = self.now()

        while self.timer_deadlines:

            deadline, timer_id = self.timer_deadlines[0]

            if deadline > now:
                break

            heapq.heappop(self.timer_deadlines)

            yield timer_id


class IOProxy:
    """
    The IOProxy class is used to interact with the mainloop about a
    single filedescriptor. The object should not be created directly,
    but always be achieved by calling the register() method of the
    Mainloop class.

    After an object is achieved, the set_interest() method can be used
    to let the mainloop know in what events we're interested.

    The set_read_handler() and set_write_handler() methods are used to
    specify which functions should be called when corrosponding event is
    raised.
    """

    def __init__(self, mainloop, fd):
        # if we are given a file or socket, call it's fileno() function
        # in order to get the filedescriptor
        fileno_func = getattr(fd, 'fileno', None)
        if fileno_func and callable(fileno_func):
            fd = fileno_func()

        self.mainloop = mainloop
        self.fd = fd
        self.opened = True

    def set_interest(self, read=None, write=None):
        """
        Let the mainloop know in which events we're interested. This is
        done by setting either read, write or both to True or False.

        When set to None, the interest will not change.
        """

        self.mainloop._update_interest(self.fd, read, write)

    def start_writing(self):
        """
        Alias for set_interest(write=True), allows for writing better
        readable code.
        """

        self.set_interest(write=True)

    def stop_writing(self):
        """
        Alias for set_interest(write=False), allows for writing better
        readable code.
        """

        self.set_interest(write=False)

    def set_read_handler(self, handler=None):
        """
        Set the function that will be called when a read event is
        raised. When handler is None, no function will be called.
        """

        self.mainloop._update_read_handler(self.fd, handler)

    def set_write_handler(self, handler=None):
        """
        Set the function that will be called when a write event is
        raised. When handler is None, no function will be called.
        """

        self.mainloop._update_write_handler(self.fd, handler)

    def unregister(self):
        """
        Disconnect the proxy from the mainloop. Deleting all references.
        """

        self.mainloop._unregister(self.fd)
        self.opened = False

    def is_open(self):
        """
        Returns wheather or not this proxy is still active, and thus
        we could expect to receive or be able to write data.
        """

        return self.opened

    def __repr__(self):
        return "<IOProxy fd={fd}>".format(
            fd=self.fd
        )


class Timer:
    """
    The Timer class is used to interact with the mainloop about timers.
    The object should not be created directly but always be achieved by
    calling the timer() method of the Mainloop class.

    After a Timer object is achieved, the set_handler method can be used
    to specify which function should be called when the timer expires.

    The set() method is used to set the timer. By calling the cancel()
    method, the current timer will be canceled.
    """

    next_timer_key = 1
    next_timer_id = 1

    def __init__(self, mainloop):
        self.mainloop = mainloop

        self.key = Timer.next_timer_key
        Timer.next_timer_key += 1

        self.timer_id = None
        self.handler = None
        self.handler_args = None
        self.handler_kwargs = None

    def set_handler(self, handler=None, *args, **kwargs):
        """
        Set the function that will be called when the timer expires.
        """

        self.handler = handler
        self.handler_args = args
        self.handler_kwargs = kwargs

    def set(self, timeout):
        """
        Sets the timer, this will cause the handler to be called after
        the given timeout expires.
        """

        self.timer_id = Timer.next_timer_id
        Timer.next_timer_id += 1

        self.mainloop._update_timer_handler(self.timer_id, self.__handler)
        self.mainloop._update_timer_timeout(self.timer_id, timeout)

    def cancel(self):
        """
        Cancel a set timer.
        """

        if self.timer_id:
            self.mainloop._update_timer_handler(self.timer_id, None)
            self.timer_id = None

    def __handler(self):
        """
        Called by the mainloop when the timer expires.
        """

        if self.handler:
            self.handler(*self.handler_args, **self.handler_kwargs)
