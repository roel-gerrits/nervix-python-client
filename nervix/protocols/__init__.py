from .nxtcp import NxtcpConnection
from .base import host_port_parser

PROTOCOL_MAP = {
    'nxtcp': (NxtcpConnection, host_port_parser),
}
