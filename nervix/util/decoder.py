from collections import deque


class BaseDecoder:
    def __init__(self, chunksize=1024):
        self.chunksize = chunksize

        self.chunkbuffer = deque()
        self.buff = bytearray()
        self.buffpos = 0
        self.autocommit_amount = 0

    def add_chunk(self, chunk):
        """
        Add a chunk of raw undecoded bytes to the internal chunkbuffer.
        """
        self.chunkbuffer.appendleft(chunk)

    def read_from_socket(self, socket, chunksize=None):
        """
        Read a chunk of raw undecoded bytes from the given socket.
        """

        if chunksize is None:
            chunksize = self.chunksize

        try:
            chunk = socket.recv(chunksize)

        except OSError:
            chunk = b''

        self.add_chunk(chunk)

        return len(chunk)

    def commit(self, amount=None):
        """
        Commit a number of bytes. This will allow for the cleanup of
        the chunkbuffer.
        """
        if amount is None:
            amount = self.autocommit_amount

        self.autocommit_amount -= amount

        del self.buff[0:amount]

    def get(self, amount, offset=0):
        """
        Return a bytes object containing the given numer of bytes.
        Returns None if the requested amount is not available.
        """
        min_buff_size = offset + amount

        while len(self.buff) < min_buff_size:

            if len(self.chunkbuffer) <= 0:
                return None

            chunk = self.chunkbuffer.pop()

            self.buff.extend(chunk)

        data = self.buff[offset:offset + amount]

        self.autocommit_amount = offset + amount

        return data

    def get_until(self, sub, limit, offset=0):
        """
        Return a bytes object containing all bytes until the specified
        sub is found.
        Returns None if the requested amount is not available.
        """
        search_offset = offset

        end = None

        while len(self.buff) < limit:

            index = self.buff.find(sub, search_offset)

            if index >= 0:
                end = index + len(sub)
                break

            if len(self.chunkbuffer) <= 0:
                return None

            chunk = self.chunkbuffer.pop()

            self.buff.extend(chunk)

        if not end:
            raise IndexError("Sub not found within limits")

        data = self.buff[offset:end]

        self.autocommit_amount = end

        return data

