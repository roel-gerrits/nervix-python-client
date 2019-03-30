
import os
from collections import deque


class Control:
    """
    Class that is used to manage control signals inside the mainloop.

    Its main purpose is to unblock the select call, and to inform
    the mainloop of some event.
    """

    def __init__(self, mainloop):

        self.control_r, self.control_w = os.pipe()
        self.events_pending = deque()
        self.events_ready = deque()

        self.proxy = mainloop.register(self.control_r)
        self.proxy.set_read_handler(self._on_event)
        self.proxy.set_interest(read=True)

    def _on_event(self):
        """
        Called when there is data available on the pipe.
        """

        buff = os.read(self.control_r, 1024)

        for b in buff:
            event = self.events_pending.popleft()
            self.events_ready.append(event)

    def signals(self):
        """
        Return the signals that been received.
        """

        while self.events_ready:
            yield self.events_ready.popleft()

    def signal(self, event):
        """
        Send a signal.
        """

        self.events_pending.append(event)

        buff = bytearray(1)
        buff[0] = event

        os.write(self.control_w, buff)
