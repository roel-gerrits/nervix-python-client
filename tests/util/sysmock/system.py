from collections import defaultdict, deque
from socket import AF_INET, SOCK_STREAM
import errno

from . import events


def log_call(func):
    def logged(*args, **kwargs):

        try:
            res = func(*args, **kwargs)

        except Exception as e:
            res = 'E:' + repr(e)
            raise e

        finally:

            print(f">>> {func.__name__}({', '.join([str(arg) for arg in args[1:]])}): {res}")

        # print(f"@    < {res}")

        return res

    return logged


class System:

    def __init__(self, eventqueue):
        self.eventqueue = eventqueue

        self.__next_fileno = 1000

        self.monotonic_time = 0.0

        self.sockets = dict()

        self.unused_local_addresses = defaultdict(deque)

    def monotonic(self):
        """ Return the current simulated monotonic timestamp.
        """

        return self.monotonic_time

    def time(self):
        """ Return the current simulated unix timestamp.
        """

        return 649692000.0 + self.monotonic_time

    def socket(self, family, kind):
        """ Create a socket, and return the new fileno.
        """

        fileno = self.__get_fileno()

        socket = Socket(family, kind)
        self.sockets[fileno] = socket

        return fileno

    def bind(self, fileno, address):
        """ Bind a socket to an address.
        """

        self.sockets[fileno].laddr = address

    def listen(self, fileno, _backlog):
        """ Start listening on the socket
        """

        self.sockets[fileno].listening = True

    def accept(self, fileno):
        """ Accept an incoming connection on the socket.
        """

        socket = self.sockets[fileno]

        if not socket.is_tcp():
            raise NotImplementedError("Only TCP is implemented at this time")

        if not socket.laddr:
            raise OSError("Socket is not bound")

        if not socket.listening:
            raise OSError("Socket is not listening")

        # first fetch the SYN event and verify it's destination address
        syn_event = self.eventqueue.fetch(
            events.IncomingTcpSyn(
                dst=socket.laddr
            )
        )

        if not syn_event:
            raise BlockingIOError("No connections to accept!")

        syn_event.done()

        # create new socket for the connection
        conn_fileno = self.__get_fileno()
        conn_socket = socket.create_connection(syn_event.src)
        self.sockets[conn_fileno] = conn_socket

        # now verify an outgoing SYN_ACK was actually expected
        self.eventqueue.expect(
            events.IncomingTcpSynAck(
                src=socket.laddr,
                dst=socket.raddr
            )
        ).done()

        return conn_fileno, syn_event.src

    def getpeername(self, fileno):
        """ Return the address of the remote peer.
        """

        socket = self.sockets[fileno]
        return socket.raddr

    def getsockname(self, fileno):
        """ Return the address of the local socket.
        """

        socket = self.sockets[fileno]
        return socket.laddr

    @log_call
    def send(self, fileno, data):
        """ Send data on the socket.
        """

        socket = self.sockets[fileno]

        output_event = self.eventqueue.expect(
            events.OutgoingTcpData(
                src=socket.laddr,
                dst=socket.raddr
            )
        )

        n = output_event.check_chunk(data)

        return n

    @log_call
    def recv(self, fileno, limit):
        """ Read data from the socket.
        """

        socket = self.sockets[fileno]

        data_event = self.eventqueue.fetch(
            events.IncomingTcpData(
                src=socket.raddr,
                dst=socket.laddr,
            )
        )

        if data_event:
            chunk = data_event.get_chunk(limit)
            return chunk

        fin_event = self.eventqueue.fetch(
            events.IncomingTcpFin(
                src=socket.raddr,
                dst=socket.laddr,
            )
        )

        if fin_event:
            return b''

        raise BlockingIOError("No data available to read")

    @log_call
    def close(self, fileno):
        """ Close the socket.
        """

        socket = self.sockets[fileno]

        if socket.connected:
            fin_event = self.eventqueue.fetch(
                events.IncomingTcpFin(
                    src=socket.raddr,
                    dst=socket.laddr,
                )
            )

            if fin_event:
                fin_event.done()

            self.eventqueue.expect(
                events.OutgoingTcpFin(
                    src=socket.laddr,
                    dst=socket.raddr
                )
            ).done()

            socket.connected = False

        socket.closed = True

    @log_call
    def connect(self, fileno, address):
        """ Connect a socket to a remote peer.
        """

        socket = self.sockets[fileno]

        if not socket.is_tcp():
            raise NotImplementedError("Only TCP is implemented at this time")

        if not socket.raddr:
            # no connection is in progress yet

            # set 'random' local address if socket was not bound yet
            if not socket.laddr:
                unused_address = self.unused_local_addresses[(socket.family, socket.kind)].pop()
                socket.laddr = unused_address

            # store socket remote address
            socket.raddr = address

            # verify that a TCP_SYN was actually expected
            self.eventqueue.expect(
                events.OutgoingTcpSyn(
                    src=socket.laddr,
                    dst=socket.raddr,
                )
            ).done()

            # connection process is in progress now
            return errno.EINPROGRESS

        else:
            # a connection is already in progress, check if there is a result yet

            syn_ack_event = self.eventqueue.fetch(
                events.IncomingTcpSynAck(
                    src=socket.raddr,
                    dst=socket.laddr,
                )
            )

            if syn_ack_event:
                syn_ack_event.done()

                socket.connected = True

                return 0  # success

            else:
                # no updates yet, return EALREADY code
                return errno.EALREADY

    @log_call
    def select(self, interest_table, timeout):
        """ Poll for read/write events on the given list of file descriptors.
        """

        # TODO: VERIFY IF WE SHOULD ADHERE TO SOME SPECIFIC ORDER!!!

        read_events = self.eventqueue.scan({
            events.IncomingTcpSyn,
            events.IncomingTcpSynAck,
            events.IncomingTcpData,
            events.IncomingTcpFin,
        })

        event_list = defaultdict(set)

        for event in read_events:

            event_type = type(event)

            for fd, socket in self.sockets.items():

                if fd not in interest_table:
                    continue

                if socket.laddr != event.dst:
                    continue

                # handle incoming SYN
                if event_type == events.IncomingTcpSyn:
                    if socket.listening:
                        if 'w' in interest_table[fd]:
                            event_list[fd].add('w')

                if socket.raddr != event.src:
                    continue

                # handle incoming SYN_ACK
                if event_type == events.IncomingTcpSynAck:
                    if 'w' in interest_table[fd]:
                        event_list[fd].add('w')

                # handle incoming FIN
                elif event_type == events.IncomingTcpFin:
                    if 'r' in interest_table[fd]:
                        event_list[fd].add('r')

                # handle incoming data
                elif event_type == events.IncomingTcpData:
                    if 'r' in interest_table[fd]:
                        event_list[fd].add('r')

                else:
                    raise NotImplementedError(f"event {event_type.__name__} is not handled")

        # all connected sockets are always allowed to write
        for fd, interest in interest_table.items():

            socket = self.sockets.get(fd, None)

            if not socket:
                continue

            if not socket.connected:
                continue

            if 'w' in interest:
                event_list[fd].add('w')

        # if there were no events we should think about sleeping for a bit
        if not event_list:
            if timeout is None:
                # no timeout is specified but if there is a Sleep event pending we will just simulate
                # a sleep for the duration of that Sleep event

                sleep_event = self.eventqueue.fetch(events.Sleep())
                if sleep_event:
                    self.monotonic_time += sleep_event.duration
                    sleep_event.done()

            else:
                # sleep for the given timeout
                self.sleep(timeout)

        return event_list.items()

    @log_call
    def sleep(self, duration):
        """ Put the process in a simulated sleep for the given duration.
        """

        sleep_event = self.eventqueue.expect(
            events.Sleep()
        )

        sleep_event.subtract_time(duration)

        self.monotonic_time += duration

    def add_unused_local_address(self, address):
        """ Inform the mocked system of an unused local address, which will be
        used by the connect() call to find a suitable address when the socket is
        not yet bound.
        """

        self.unused_local_addresses[(address.family, address.kind)].append(address.address)

    """
    Helper functions
    """

    def __get_fileno(self):
        """ Generate a new fileno
        """

        fileno = self.__next_fileno
        self.__next_fileno += 1
        return fileno


class Socket:

    def __init__(self, family, kind):
        self.family = family
        self.kind = kind

        self.laddr = None
        self.raddr = None

        self.listening = False
        self.connected = False
        self.closed = False

    def is_tcp(self):
        return self.family == AF_INET \
               and self.kind == SOCK_STREAM

    def create_connection(self, raddr):
        s = Socket(self.family, self.kind)
        s.laddr = self.laddr
        s.raddr = raddr
        return s
