from collections import defaultdict, deque
from socket import AF_INET, SOCK_STREAM
import errno

from . import events


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

        # first get the SYN event from the queue so that we learn the peer address
        syn_event = self.eventqueue.peek(events.DoTcpSyn)
        peer_addr = syn_event.src

        # now verify that same event, so we know for sure that the dst address matches the socket
        self.eventqueue.verify(
            events.DoTcpSyn,
            src=peer_addr,
            dst=socket.laddr,
        )

        # and then verify the SYN_ACK event
        self.eventqueue.verify(
            events.ExpectTcpSynAck,
            src=socket.laddr,
            dst=peer_addr,
        )

        # create new socket and set both laddr and raddr
        conn_fileno = self.__get_fileno()
        conn_socket = socket.create_connection()
        conn_socket.raddr = peer_addr
        self.sockets[conn_fileno] = conn_socket

        return conn_fileno, peer_addr

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

    def send(self):
        """ Send data on the socket.
        """

        # todo: verify tcp_output

    def recv(self):
        """ Read data from the socket.
        """

        # scan input queue for matching data and return a chunk

    def close(self):
        """ Close the socket.
        """

        # todo: verify tcp_rst if the socket is connected

    def connect(self, fileno, address):
        """ Connect a socket to a remote peer.
        """

        socket = self.sockets[fileno]

        if not socket.is_tcp():
            raise NotImplementedError("Only TCP is implemented at this time")

        if not socket.raddr:
            # in case no connection was in progress yet

            if not socket.laddr:
                unused_address = self.unused_local_addresses[(socket.family, socket.kind)].pop()

                socket.laddr = unused_address

            socket.raddr = address

            self.eventqueue.verify(
                events.ExpectTcpSyn,
                src=socket.laddr,
                dst=socket.raddr
            )

            # connection process is in progress now
            return errno.EINPROGRESS

        else:
            # a connection is already in progress, check if there is a result yet

            syn_ack_event = self.eventqueue.peek(events.DoTcpSynAck)

            if syn_ack_event:
                self.eventqueue.verify(
                    events.DoTcpSynAck,
                    src=socket.raddr,
                    dst=socket.laddr
                )

                # yes, there was a SYN_ACK event and it was valid, return successcode
                return 0  # success

            else:
                # no updates yet, return EALREADY code
                return errno.EALREADY

    def select(self, interest_table, timeout):
        """ Poll for read/write events on the given list of file descriptors.
        """

        # TODO: VERIFY IF WE SHOULD ADHERE TO SOME SPECIFIC ORDER!!!

        read_events = self.eventqueue.get_events({
            events.DoTcpSyn, events.DoTcpSynAck, events.DoTcpInput, events.DoTcpRst
        })

        event_list = defaultdict(set)

        for event in read_events:

            for fd, socket in self.sockets.items():

                if 'r' in interest_table[fd]:

                    if fd not in interest_table:
                        continue

                    if socket.laddr != event.dst:
                        continue

                    if socket.listening and type(event) == events.DoTcpSyn:
                        event_list[fd].add('r')

                    elif socket.raddr == event.src:
                        event_list[fd].add('r')

        # always allow writing to sockets
        for fd, interest in interest_table.items():

            if 'w' in interest:
                event_list[fd].add('w')

        if not event_list:
            if timeout is not None:
                self.sleep(timeout)

        return event_list.items()

    def sleep(self, duration):
        """ Put the process in a simulated sleep for the given duration.
        """

        self.eventqueue.verify(
            events.ExpectSleep,
            duration=duration
        )

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
        self.closed = False

    def is_tcp(self):
        return self.family == AF_INET \
               and self.kind == SOCK_STREAM

    def create_connection(self):
        s = Socket(self.family, self.kind)
        s.laddr = self.laddr
        return s
