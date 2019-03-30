from socket import AF_INET, SOCK_STREAM


class Address:

    def __init__(self, family, kind, address):
        self.family = family
        self.kind = kind
        self.address = address


class TcpAddress(Address):

    def __init__(self, host, port):
        Address.__init__(self, AF_INET, SOCK_STREAM, (host, port))
