"""
This file describes the format of all the packets that are defined in the NXTCP protocol.
The functions in this module are used to create test packets used in the unittests.
"""

from struct import pack

# UPSTREAM
PACKET_LOGIN = 0x01
PACKET_LOGOUT = 0x03
PACKET_REQUEST = 0x04
PACKET_POST = 0x06
PACKET_SUBSCRIBE = 0x08
PACKET_UNSUBSCRIBE = 0x10
PACKET_PONG = 0x81
PACKET_QUIT = 0x84

# DOWNSTREAM
PACKET_SESSION = 0x02
PACKET_CALL = 0x05
PACKET_MESSAGE = 0x07
PACKET_INTEREST = 0x09
PACKET_PING = 0x80
PACKET_WELCOME = 0x82
PACKET_BYEBYE = 0x83

"""
Functions that create packets.
Send from CLIENT -> SERVER
"""


def login(name, persist, standby, enforce):
    flags = 0b000

    if persist:
        flags |= 0b001

    if standby:
        flags |= 0b010

    if enforce:
        flags |= 0b100

    n = 2 + len(name)

    return uint32(n) + uint8(PACKET_LOGIN) + uint8(flags) + string(name)


def logout(name):
    n = 1 + len(name)
    return uint32(n) + uint8(PACKET_LOGOUT) + string(name)


def request(name, unidirectional, messageref, timeout_ms, payload):
    flags = 0b0

    if unidirectional:
        flags |= 0b1

    n = 14 + len(name) + len(payload)

    return uint32(n) + uint8(PACKET_REQUEST) + string(name) + uint8(flags) + uint32(messageref) + uint32(
        timeout_ms) + blob(
        payload)


def post(postref, payload):
    n = 8 + len(payload)

    return uint32(n) + uint8(PACKET_POST) + uint32(postref) + blob(payload)


def subscribe(messageref, name, topic):
    n = 9 + len(name) + len(topic)

    return uint32(n) + uint8(PACKET_SUBSCRIBE) + uint32(messageref) + string(name) + blob(topic)


def unsubscribe(name, topic):
    n = 5 + len(name) + len(topic)
    return uint32(n) + uint8(PACKET_UNSUBSCRIBE) + string(name) + blob(topic)


def pong():
    n = 0
    return uint32(n) + uint8(PACKET_PONG)


def quit():
    return uint32(0) + uint8(PACKET_QUIT)


"""
Functions that create packets.
Send from SERVER -> CLIENT
"""

SESSION_STATE_ENDED = 0
SESSION_STATE_STANDBY = 1
SESSION_STATE_ACTIVE = 2


def session(name, state):
    n = 2 + len(name)
    return uint32(n) + uint8(PACKET_SESSION) + uint8(state) + string(name)


def call(unidirectional, postref, name, payload):
    flags = 0b0

    if unidirectional:
        flags |= 0b1

    n = 10 + len(name) + len(payload)
    return uint32(n) + uint8(PACKET_CALL) + uint8(flags) + uint32(postref) + string(name) + blob(payload)


MESSAGE_STATUS_OK = 0
MESSAGE_STATUS_TIMEOUT = 1
MESSAGE_STATUS_UNREACHABLE = 2


def message(messageref, status, payload=None):
    n = 5

    pkt = uint8(PACKET_MESSAGE) + uint8(status) + uint32(messageref)

    if status == MESSAGE_STATUS_OK:
        n += 4 + len(payload)

        pkt += blob(payload)

    return uint32(n) + pkt


INTEREST_STATUS_NOINTEREST = 0
INTEREST_STATUS_INTEREST = 1


def interest(postref, name, status, topic):
    n = 10 + len(topic) + len(name)
    return uint32(n) + uint8(PACKET_INTEREST) + uint8(status) + uint32(postref) + string(name) + blob(topic)


def ping():
    n = 0
    return uint32(n) + uint8(PACKET_PING)


def welcome():
    server_version = 1
    protocol_version = 1
    return uint32(8) + uint8(PACKET_WELCOME) + uint32(server_version) + uint32(protocol_version)


def byebye():
    return uint32(0) + uint8(PACKET_BYEBYE)


"""
Helper functions to help construct packets
"""


def uint32(val):
    return pack('>I', val)


def uint8(val):
    return pack('>B', val)


def string(val):
    return pack('>B', len(val)) + val


def blob(val):
    return pack('>I', len(val)) + val
