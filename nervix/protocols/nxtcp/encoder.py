from struct import pack_into, pack

from nervix.util.encoder import BaseEncoder

from .defines import *


class Encoder(BaseEncoder):

    def encode(self, packet):
        """
        Encode a packet object and append it to the internal
        chunkbuffer.
        """

        chunk = packet.get_chunk()

        self.add_encoded_chunk(chunk)


class BasePacket:

    def __init__(self):
        self.chunk = bytearray(5)

    def get_chunk(self):
        """
        Return a bytes object which contains the encoded data.
        """

        length = len(self.chunk) - 5

        pack_into('>i', self.chunk, 0, length)

        return bytes(self.chunk)

    def set_type(self, packettype):
        """
        Set the packet type.
        """
        self.chunk[4] = packettype

    def add_field(self, value=[]):
        """
        Add a field to the packet.
        """
        self.chunk.extend(value)

    def add_uint8_field(self, value):
        """
        Add a uint8 field to the packet.
        """
        self.add_field([value])

    def add_uint32_field(self, value):
        """
        Add a unit32 field to the packet.
        """
        self.add_field(pack('>I', value))

    def add_string_field(self, value):
        """
        Add a string field to the packet.
        """
        self.chunk.append(len(value))
        self.chunk.extend(value)

    def add_blob_field(self, value):
        """
        Add a blob field to the packet.
        """
        self.add_uint32_field(len(value))
        self.chunk.extend(value)


class LoginPacket(BasePacket):
    """
    uint8: flags
        0: persist
        1: standby
        2: enforce
    string: name
    """

    def __init__(self, name, enforce, standby, persist):
        BasePacket.__init__(self)

        # set type
        self.set_type(PACKET_LOGIN)

        # write flags
        flags = 0
        flags |= (persist << 0)
        flags |= (standby << 1)
        flags |= (enforce << 2)
        self.add_uint8_field(flags)

        # write name
        self.add_string_field(name)


class LogoutPacket(BasePacket):
    """
    string: name
    """

    def __init__(self, name):
        BasePacket.__init__(self)

        # set type
        self.set_type(PACKET_LOGOUT)

        # write name
        self.add_string_field(name)


class RequestPacket(BasePacket):
    """
    string: name
    uint8: flags:
        0: unidirectional
    uint32: messageref
    uint32: timeout
    blob: payload
    """

    def __init__(self, name, unidirectional, messageref, timeout, payload):
        BasePacket.__init__(self)

        # set type
        self.set_type(PACKET_REQUEST)

        # write name
        self.add_string_field(name)

        # write flags
        flags = 0
        flags |= (unidirectional)
        self.add_uint8_field(flags)

        # write messageref
        self.add_uint32_field(0 if unidirectional else messageref)

        # write timeout
        self.add_uint32_field(encode_timeout(timeout))

        # write pay load
        self.add_blob_field(payload)


class PostPacket(BasePacket):
    """
    uint32: postref
    blob: payload
    """

    def __init__(self, postref, payload):
        BasePacket.__init__(self)

        # set type
        self.set_type(PACKET_POST)

        # add postref
        self.add_uint32_field(postref)

        # add payload
        self.add_blob_field(payload)


class SubscribePacket(BasePacket):
    """
    uint32: messageref
    string: name
    blob: topic
    """

    def __init__(self, messageref, name, topic):
        BasePacket.__init__(self)

        # set type
        self.set_type(PACKET_SUBSCRIBE)

        # add messageref
        self.add_uint32_field(messageref)

        # add name
        self.add_string_field(name)

        # add topic
        self.add_blob_field(topic)


class UnsubscribePacket(BasePacket):
    """
    string: name
    blob: topic
    """

    def __init__(self, name, topic):
        BasePacket.__init__(self)

        # set type
        self.set_type(PACKET_UNSUBSCRIBE)

        # add name
        self.add_string_field(name)

        # add topic
        self.add_blob_field(topic)


class PongPacket(BasePacket):
    """
    blob: payload
    """

    def __init__(self):
        BasePacket.__init__(self)

        # set type
        self.set_type(PACKET_PONG)


class QuitPacket(BasePacket):
    """
    -
    """

    def __init__(self):
        BasePacket.__init__(self)

        # set type
        self.set_type(PACKET_QUIT)


class SyncReqPacket(BasePacket):
    """
    -
    """

    def __init__(self):
        BasePacket.__init__(self)

        # set type
        self.set_type(PACKET_SYNC_REQ)


def encode_name(name):
    # convert to bytes if needed
    if isinstance(name, str):
        name = name.encode()

    return name


def encode_timeout(timeout):
    if timeout is None:
        timeout = 0

    else:
        timeout = int(timeout * 1000)

    return timeout


def encode_payload(payload):
    # convert to bytes if needed
    if isinstance(payload, str):
        payload = payload.encode()

    return payload
