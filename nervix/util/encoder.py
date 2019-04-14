from collections import deque
import logging

logger = logging.getLogger(__name__)


class BaseEncoder:
    def __init__(self, chunksize=1024):
        self.chunksize = chunksize

        self.chunkbuffer = deque()
        self.currentchunk = None
        self.fetchpos = None
        self.commitpos = None

    def write_to_socket(self, socket, chunksize=None):
        """
        Write bytes from the internal chunkbuffer to the given socket.
        The number of written bytes is limited by the given chunksize,
        if not given, the default chunksize (specified via __init__)
        will be used.

        Returns True if ...
        """

        chunk = self.fetch_chunk(chunksize)

        n = 0

        if chunk:
            n = socket.send(chunk)
            self.commit(n)

        return n

    def fetch_chunk(self, chunksize=None):
        """
        Returns a chunk of bytes with maximum length of chunksize.
        Returns None if no chunks are available.

        An empty bytes object is returned if all bytes from the current
        chunk are fetched but not commited yet. The user should first
        commit all fetched bytes.
        """

        if not chunksize:
            chunksize = self.chunksize

        if not self.currentchunk:

            if len(self.chunkbuffer) > 0:
                self.currentchunk = self.chunkbuffer.pop()
                self.fetchpos = 0
                self.commitpos = 0

            else:
                return None

        startpos = self.commitpos

        endpos = min(
            startpos + len(self.currentchunk),
            startpos + chunksize
        )

        chunk = self.currentchunk[startpos:endpos]

        self.fetchpos += len(chunk)

        return chunk

    def commit(self, amount):
        """
        Commit a number of bytes. This will allow for cleanup of the
        chunkbuffer.
        """

        self.commitpos += amount

        if self.commitpos == len(self.currentchunk):
            self.currentchunk = None

        elif self.commitpos > self.fetchpos:
            raise ValueError(
                "Commit of {} bytes is not possible as that number of bytes are not fetched yet".format(amount))

    def add_encoded_chunk(self, chunk):
        """
        Add a chunk of bytes to the internal chunkbuffer. The argument
        must be a bytes object.
        """

        if not isinstance(chunk, bytes):
            raise TypeError("Given chunk is not an instance of bytes")

        self.chunkbuffer.appendleft(chunk)
