class BaseConnection:
    """ Interface for all connection subclasses.
    """

    def set_ready_handler(self, handler):
        raise NotImplementedError()

    def set_downstream_handler(self, handler):
        raise NotImplementedError()

    def send_verb(self, verb):
        raise NotImplementedError()


def host_port_parser(address_str):
    """ Function to parse an address string and return a (host, port) tuple.
    Raises ValueError if the given string is invalid.
    """

    host_pos = address_str.find(':')

    if host_pos == -1:
        raise ValueError("No collon found in address")

    host = address_str[0:host_pos]

    port_str = address_str[host_pos + 1:]

    try:
        port = int(port_str)

    except ValueError:
        raise ValueError(f"Invalid address {address_str}")

    return host, port
