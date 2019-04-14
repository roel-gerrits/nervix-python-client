from .system import System
from . import events


class Sysmock:

    def __init__(self):
        self.eventqueue = events.EventQueue()
        self.system = System(self.eventqueue)

    def expect_sleep(self, duration):
        """ Expect the local process to sleep for the given duration.
        """

        self.eventqueue.add(events.Sleep(
            duration=duration
        ))

    def expect_tcp_syn(self, src, dst):
        """ Expect the local process to send a TCP SYN packet, meaning, it
        should do an connect().
        """

        self.eventqueue.add(events.OutgoingTcpSyn(
            src=src.address,
            dst=dst.address,
        ))

    def expect_tcp_syn_ack(self, src, dst):
        """ Expect the local process to send a TCP SYN ACK packet, meaning it
        should do an accept() .
        """

        self.eventqueue.add(events.OutgoingTcpSynAck(
            src=src.address,
            dst=dst.address,
        ))

    def expect_tcp_output(self, src, dst, data):
        """ Expect the local process to send some TCP data, meaning it should do
        a send().
        """

        self.eventqueue.add(events.OutgoingTcpData(
            src=src.address,
            dst=dst.address,
            data=data,
        ))

    def expect_tcp_fin(self, src, dst):
        """ Expect the local process to send a TCP RST packet, meaning it should
        do a close().
        """

        self.eventqueue.add(events.OutgoingTcpFin(
            src=src.address,
            dst=dst.address
        ))

    def do_tcp_syn(self, src, dst):
        """ Simulate a remote TCP SYN packet. This will cause a read event on a
        listening socket.
        """

        self.eventqueue.add(events.IncomingTcpSyn(
            src=src.address,
            dst=dst.address,
        ))

    def do_tcp_syn_ack(self, src, dst):
        """ Simulate a remote TCP SYN ACK packet. This will cause the connect
        call to return successfully.
        """

        self.eventqueue.add(events.IncomingTcpSynAck(
            src=src.address,
            dst=dst.address,
        ))

    def do_tcp_input(self, src, dst, data):
        """ Simulate a TCP data packet. This will cause a read event on a
        connected socket.
        """

        self.eventqueue.add(events.IncomingTcpData(
            src=src.address,
            dst=dst.address,
            data=data,
        ))

    def do_tcp_fin(self, src, dst):
        """ Simulate a TCP RST packet. This will cause a read event on a
        connected socket, when read() is called zero bytes are returned (b'')
        """

        self.eventqueue.add(events.IncomingTcpFin(
            src=src.address,
            dst=dst.address
        ))

    def events_pending(self):
        """ Returns True if there are are any un-verified expectations, OR if
        there is are any unread events available for the process to read.
        """

        return len(self.eventqueue) > 0

    def assert_events_done(self):
        """ Assert that there are no more pending events"""

        if self.events_pending():
            self.eventqueue.dump()
            raise AssertionError("Not all events are processed yet")

    def run_events(self, runfunc, limit=10):
        """ Execute the given function in a loop until all events are processed.
        If nothing happens for limit iterations an error will be raised.
        """

        current_len = len(self.eventqueue)
        prev_len = current_len
        limit_left = limit

        while limit_left > 0:

            # execute the function
            runfunc()

            current_len = len(self.eventqueue)

            if current_len == 0:
                break

            elif current_len != prev_len:
                limit_left = limit
                prev_len = current_len

            else:
                limit_left -= 1

        else:
            self.eventqueue.dump()
            raise AssertionError(f"Nothing happened withing {limit} iterations")
