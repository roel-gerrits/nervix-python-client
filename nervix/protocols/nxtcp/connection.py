from nervix.protocols.base import BaseConnection


class NxtcpConnection(BaseConnection):

    def __init__(self, mainloop, address):
        self.address = address

    def set_ready_handler(self, handler):
        pass

    def set_downstream_handler(self, handler):
        pass

    def send_verb(self, verb):
        pass
