from .system import System
from . import events


class Sysmock:

    def __init__(self):
        self.eventqueue = events.EventQueue()
        self.system = System(self.eventqueue)

    def expect_sleep(self, duration):
        """ Expect the local process to sleep for the given duration.
        """

        self.eventqueue.append(events.ExpectSleep(
            duration=duration,
        ))

    def expect_tcp_syn(self, src, dst):
        """ Expect the local process to send a TCP SYN packet, meaning, it
        should do an connect().
        """

        self.eventqueue.append(events.ExpectTcpSyn(
            src=src.address,
            dst=dst.address,
        ))

    def expect_tcp_syn_ack(self, src, dst):
        """ Expect the local process to send a TCP SYN ACK packet, meaning it
        should do an accept() .
        """

        self.eventqueue.append(events.ExpectTcpSynAck(
            src=src.address,
            dst=dst.address,
        ))

    def expect_tcp_output(self, src, dst, data):
        """ Expect the local process to send some TCP data, meaning it should do
        a send().
        """

    def expect_tcp_rst(self, src, dst):
        """ Expect the local process to send a TCP RST packet, meaning it should
        do a close().
        """

    def do_tcp_syn(self, src, dst):
        """ Simulate a remote TCP SYN packet. This will cause a read event on a
        listening socket.
        """

        self.eventqueue.append(events.DoTcpSyn(
            src=src.address,
            dst=dst.address,
        ))

    def do_tcp_syn_ack(self, src, dst):
        """ Simulate a remote TCP SYN ACK packet. This will cause the connect
        call to return successfully.
        """

        self.eventqueue.append(events.DoTcpSynAck(
            src=src.address,
            dst=dst.address,
        ))

    def do_tcp_input(self, src, dst, data):
        """ Simulate a TCP data packet. This will cause a read event on a
        connected socket.
        """

    def do_tcp_rst(self, src, dst):
        """ Simulate a TCP RST packet. This will cause a read event on a
        connected socket, when read() is called zero bytes are returned (b'')
        """

    def events_pending(self):
        """ Returns True if there are are any un-verified expectations, OR if
        there is are any unread events available for the process to read.
        """

        return len(self.eventqueue) > 0
