from nervix.mainloop import Mainloop
from nervix.protocols import PROTOCOL_MAP
from nervix.channel import Channel


def create_channel(uri, loop=None):
    """ Creates Channel instance based on the given uri.
    If no mainloop is given a new Mainloop will be created.
    Returns a (mainloop, channel) tuple.
    """

    if not loop:
        loop = Mainloop()

    connection = create_connection(loop, uri)
    chan = Channel(connection)
    return loop, chan


def create_connection(loop, uri):
    """ Create a connection instance based on the given uri.
    Raises ValueError if the given uri was not valid.
    """

    proto_pos = uri.find('://')
    protocol_name = uri[0:proto_pos]

    if protocol_name not in PROTOCOL_MAP:
        raise ValueError("Unknown protocol %s" % protocol_name)

    address_str = uri[proto_pos + 3:]

    protocol_cls, address_parser = PROTOCOL_MAP[protocol_name]

    address = address_parser(address_str)

    connection = protocol_cls(loop, address)

    return connection
