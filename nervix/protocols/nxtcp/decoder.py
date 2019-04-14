from struct import unpack_from

from nervix.util.decoder import BaseDecoder

from .defines import *


class Decoder(BaseDecoder):
    def __init__(self, *args, **kwargs):
        BaseDecoder.__init__(self, *args, **kwargs)

        self.handler_map = {
            PACKET_SESSION: SessionPacket,
            PACKET_CALL: CallPacket,
            PACKET_MESSAGE: MessagePacket,
            PACKET_INTEREST: InterestPacket,
            PACKET_PING: PingPacket,
            PACKET_WELCOME: WelcomePacket,
            PACKET_BYEBYE: ByeByePacket,
        }

    def decode(self):
        """
        Decode a single packet from the chunks that are currently
        present in the chunkbuffer.

        Returns None if no packet could be constructed.
        """

        header = self.get(5)

        if not header:
            return

        length, packet_type = unpack_from('>IB', header)

        frame = self.get(length, 5)

        if frame is None:
            return

        self.commit()

        handler = self.handler_map.get(packet_type, None)

        if not handler:
            raise DecodingError('Unknown packet type 0x{:02x}'.format(packet_type))

        packet = handler(frame)

        return packet


class DecodingError(RuntimeError):
    pass


class BasePacket:
    def __init__(self, frame):
        self.frame = frame
        self.nextbyte = 0

    def get_uint8(self, offset):
        self.nextbyte = offset + 1
        return unpack_from('>B', self.frame, offset)[0]

    def get_uint32(self, offset):
        self.nextbyte = offset + 4
        return unpack_from('>I', self.frame, offset)[0]

    def get_uint64(self, offset):
        self.nextbyte = offset + 8
        return unpack_from('>Q', self.frame, offset)[0]

    def get_string(self, offset):
        length = self.get_uint8(offset)

        start = offset + 1
        end = offset + 1 + length

        if end > len(self.frame):
            raise DecodingError('String size of {:d} exceeds frame size {:d}'.format(length, len(self.frame)))

        self.nextbyte = end

        return bytes(self.frame[start: end])

    def get_blob(self, offset):
        length = self.get_uint32(offset)

        start = offset + 4
        end = offset + 4 + length

        if end > len(self.frame):
            raise DecodingError('Blob size of {:d} exceeds frame size {:d}'.format(length, len(self.frame)))

        self.nextbyte = end

        return bytes(self.frame[start: end])


class SessionPacket(BasePacket):
    """
    uint8: state:
        0: ended
        1: standby
        2: active
    string: name
    """

    STATE_ENDED = 0
    STATE_STANDBY = 1
    STATE_ACTIVE = 2

    def __init__(self, frame):
        BasePacket.__init__(self, frame)

        self.state = self.get_uint8(self.nextbyte)
        self.name = self.get_string(self.nextbyte)

    def __repr__(self):
        return 'SessionPacket(name={name}, state={state})'.format(
            name=self.name,
            state=['ENDED', 'STANDBY', 'ACTIVE'][self.state]
        )


class CallPacket(BasePacket):
    """
    uint8: flags
        0: directional
        1: unidirectional
    uint32: postref
    string: name
    blob: payload
    """

    def __init__(self, frame):
        BasePacket.__init__(self, frame)

        flags = self.get_uint8(self.nextbyte)
        self.unidirectional = (flags & (1 << 0)) > 0

        self.postref = self.get_uint32(self.nextbyte)

        self.name = self.get_string(self.nextbyte)

        self.payload = self.get_blob(self.nextbyte)


class MessagePacket(BasePacket):
    """
    uint8: status
        0: ok
        1: timeout
        2: unreachable
    uint32: messageref
    blob: payload (if state == ok)
    """

    STATUS_OK = 0
    STATUS_TIMEOUT = 1
    STATUS_UNREACHABLE = 2

    def __init__(self, frame):
        BasePacket.__init__(self, frame)

        self.status = self.get_uint8(self.nextbyte)

        self.messageref = self.get_uint32(self.nextbyte)

        self.payload = None
        if self.status == self.STATUS_OK:
            self.payload = self.get_blob(self.nextbyte)


class InterestPacket(BasePacket):
    """
    uint8: status
        0: no interest
        1: interest
    uint32: postref
    blob: topic
    """

    STATUS_NO_INTEREST = 0
    STATUS_INTEREST = 1

    def __init__(self, frame):
        BasePacket.__init__(self, frame)

        self.status = self.get_uint8(self.nextbyte)

        self.postref = self.get_uint32(self.nextbyte)

        self.name = self.get_string(self.nextbyte)

        self.topic = self.get_blob(self.nextbyte)


class PingPacket(BasePacket):
    """
    blob: payload
    """

    def __init__(self, frame):
        BasePacket.__init__(self, frame)


class WelcomePacket(BasePacket):
    """
    uint32: server_version
    uint32: protocol_version
    """

    def __init__(self, frame):
        BasePacket.__init__(self, frame)

        self.server_version = self.get_uint32(self.nextbyte)
        self.protocol_version = self.get_uint32(self.nextbyte)


class ByeByePacket(BasePacket):
    """
    -
    """

    def __init__(self, frame):
        BasePacket.__init__(self, frame)
