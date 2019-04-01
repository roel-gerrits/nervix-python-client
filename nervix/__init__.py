from nervix.mainloop import Mainloop
from nervix.protocols import PROTOCOL_MAP
from nervix.channel import Channel


def create_channel(uri):
    mainloop = Mainloop()
    connection = create_connection(mainloop, uri)
    channel = Channel(connection)
    return mainloop, channel


def create_connection(mainloop, uri):
    """
    """

    proto_pos = uri.find('://')
    protocol_name = uri[0:proto_pos]

    if protocol_name not in PROTOCOL_MAP:
        raise ValueError("Unknown protocol %s" % protocol_name)

    address_str = uri[proto_pos + 2:]

    protocol_cls, address_parser = PROTOCOL_MAP[protocol_name]

    address = address_parser(address_str)

    connection = protocol_cls(mainloop, address)

    return connection
